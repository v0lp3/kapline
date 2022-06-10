import json
import os
import shutil

import pyspark
import pyspark.sql.types as tp
import requests

from pyspark.ml import PipelineModel

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, struct, to_json, udf

MODEL_DIR = "./model"
KAFKA_HOSTS = os.environ["KAFKA_HOSTS"]
FILE_HOST = os.environ["FILE_HOST"]

TOKEN = os.environ["TOKEN"]
TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

QUARK_PATH = "/.quark-engine/quark-rules"

"""
Load label associated to quark-rules, so we can calc
some stats after
"""
action_labels = {}

with open(os.path.join(QUARK_PATH, "label_desc.csv"), "r") as fp:
    for line in fp.readlines()[1:]:
        action_labels[line.split(",")[0]] = []

for rule in os.listdir(os.path.join(QUARK_PATH, "rules")):
    with open(os.path.join(QUARK_PATH, "rules", rule)) as r:
        tmp = json.load(r)

    for i in tmp["label"]:
        if i not in action_labels:
            action_labels[i] = []

        action_labels[i].append(int(rule.split(".json")[0]))

attributes = {
    "timestamp": tp.StructField(
        name="@timestamp", dataType=tp.TimestampType(), nullable=False
    ),
    "filename": tp.StructField(
        name="filename", dataType=tp.StringType(), nullable=False
    ),
    "userid": tp.StructField(name="userid", dataType=tp.LongType(), nullable=False),
    "md5": tp.StructField(name="md5", dataType=tp.StringType(), nullable=False),
    "features": tp.StructField(
        name="features", dataType=tp.ArrayType(tp.DoubleType()), nullable=False
    ),
    "size": tp.StructField(name="size", dataType=tp.LongType(), nullable=False),
    "label": tp.StructField(name="label", dataType=tp.StringType(), nullable=False),
}


def build_struct(attrs: []) -> tp.StructType:
    struct = tp.StructType()

    for attr in attrs:
        struct = struct.add(attributes[attr])

    return struct


"""
raw_data_struct is a struct that contains raw data. The filename is used as url to download the
file and process it.
"""

raw_attrs = ["timestamp", "filename", "userid", "md5"]
raw_data_struct = build_struct(raw_attrs)


"""
report_struct is a struct that contains some informations that
we extract from the quark-engine report. 
"""

reports_attrs = ["features", "size"]
report_struct = build_struct(reports_attrs)


"""
elastic_struct is a struct that contains the data ready
to be saved into elastic search 
"""

elastic_attrs = raw_attrs + reports_attrs + ["label"]
elastic_struct = build_struct(elastic_attrs)


spark = pyspark.sql.SparkSession.builder.appName("kapline").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# loads trained model

shutil.unpack_archive(filename="model.zip", extract_dir=MODEL_DIR)
trainedModel = PipelineModel.load(os.path.join(MODEL_DIR, "apkAnalysis"))


def enrich_dataframe(df: DataFrame) -> DataFrame:
    """ "
    Creates a dataframe that contains both raw data and quark-engine
    report on that file.
    """

    def analyze_file(filename: str) -> dict:
        """
        Get the file from the dataserver and analyze it through quark-engine.
        Uses filename as route to query the file.
        """

        with open(filename, "wb") as tmp:
            response = requests.get(f"http://{FILE_HOST}/{filename}").content
            tmp.write(response)

        os.system(f"quark -a {filename} -o {filename}.json")

        with open(f"{filename}.json", "r") as report:
            filereport = json.load(report)

        return filereport

    def filter_crime(crime: dict) -> tuple:
        """
        Filter function that returns the score for a crime.
        """

        rule = int(crime["rule"].split(".json")[0])
        weight = crime["weight"]

        return (rule, weight)

    @udf
    def get_features(filename: str) -> str:
        report = analyze_file(filename)

        features = [filter_crime(crime) for crime in report["crimes"]]

        features.sort(key=lambda x: x[0])

        # model is trained for only 204 rules :(
        features = features[:204]

        # idk if this is the best practice but it works
        return json.dumps(
            {
                "features": [crime_score[1] for crime_score in features],
                "size": report["size_bytes"],
            }
        )

    df = df.withColumn(
        "output", from_json(get_features(df.filename), schema=report_struct)
    ).select("@timestamp", "filename", "userid", "md5", "output.*")

    return df


def predict(df: DataFrame, model: PipelineModel):
    @udf
    def send_telegram_notification(userid, md5, label) -> str:
        """
        Send the report to the user
        """

        text = f"{label}"

        data = {
            "chat_id": userid,
            "parse_mode": "MarkdownV2",
            "text": text,
        }

        requests.post(TELEGRAM_API, data)

        return md5

    to_vector = udf(lambda a: Vectors.dense(a), VectorUDT())

    df = df.withColumn("features", to_vector(df.features))

    df = model.transform(df)

    to_array = udf(lambda v: v.toArray().tolist(), tp.ArrayType(tp.DoubleType()))

    df = (
        df.withColumn(
            "md5", send_telegram_notification(df.userid, df.md5, df.predictedLabel)
        )
        .withColumn("features", to_array(df.features))
        .select(
            "@timestamp",
            "filename",
            "userid",
            "md5",
            "features",
            "size",
            "predictedLabel",
        )
    )

    return df


def get_message(df: DataFrame, schema: tp.StructType) -> DataFrame:
    return (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema=schema).alias("data"))
        .select("data.*")
    )


def process_message_pointer(df: DataFrame, model: PipelineModel = trainedModel):
    df = get_message(df, raw_data_struct)
    df = enrich_dataframe(df)
    df = predict(df, model)

    return df


def statistics(df: DataFrame):
    @udf
    def get_total_score():
        pass


def prepare_for_elastic(df: DataFrame):
    df = get_message(df, elastic_struct)
    return df


"""
In the first topic we want to retrieve the file from
the dataserver and process it to extract features

The workflow is:

=> kafka -> spark -> kafka
"""

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("subscribe", "apk_pointers")
    .load()
)


df = process_message_pointer(df)

query_kafka = (
    df.select(to_json(struct("*")).alias("value"))
    .selectExpr("CAST(value AS STRING)")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("topic", "analyzed")
    .option("checkpointLocation", "./checkpoints-api")
    .start()
)

"""
In the second topic we want to enrich the dataframe
with some statistics

The workflow is:

=> kafka -> spark -> elastic search
"""

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("subscribe", "analyzed")
    .load()
)

df = prepare_for_elastic(df)

query_elastic = df.writeStream.format("console").start()

query_elastic.awaitTermination()

query_kafka.awaitTermination()
