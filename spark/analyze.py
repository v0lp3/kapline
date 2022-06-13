import json
import os
import shutil

import pyspark
import pyspark.sql.types as tp
import requests

from pyspark.ml import PipelineModel

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, struct, to_json, udf, aggregate, lit

from pyspark.conf import SparkConf

MODEL_DIR = "./model"
KAFKA_HOSTS = os.environ["KAFKA_HOSTS"]
ELASTIC_HOST = os.environ["ELASTIC_HOST"]
ELASTIC_PORT = os.environ["ELASTIC_PORT"]
ELASTIC_PASSWORD = os.environ["ELASTIC_PASSWORD"]
HTTPD_HOST = os.environ["HTTPD_HOST"]

TOKEN = os.environ["TOKEN"]
TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

MAX_RULES = 204

with open("quark_labels.json", "r") as f:
    quark_labels = json.load(f)


attributes = {
    "timestamp": tp.StructField(
        name="timestamp", dataType=tp.StringType(), nullable=False
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
    "label": tp.StructField(
        name="predictedLabel", dataType=tp.StringType(), nullable=False
    ),
    "custom": lambda x: tp.StructField(
        name=x, dataType=tp.DoubleType(), nullable=False
    ),
}


def build_struct(attrs: list) -> tp.StructType:
    struct = tp.StructType()

    for attr in attrs:

        if attr not in attributes:
            field = attributes["custom"](attr)
        else:
            field = attributes[attr]

        struct.add(field)

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

elastic_partial_attrs = ["timestamp", "md5", "features", "size", "label"]
elastic_struct = build_struct(elastic_partial_attrs)


"""
label_struct is a struct that contains the quark-engine labels 
"""
label_attrs = [f"{label}_score" for label in list(quark_labels)]
label_struct = build_struct(label_attrs)


sparkConf = (
    SparkConf()
    .set("spark.scheduler.mode", "FAIR")
    .set("es.nodes.wan.only", "true")
    .set("es.net.ssl", "true")
    .set("es.net.ssl.cert.allow.self.signed", "true")
    .set("es.nodes", ELASTIC_HOST)
    .set("es.port", ELASTIC_PORT)
    .set("es.net.http.auth.user", "elastic")
    .set("es.net.http.auth.pass", ELASTIC_PASSWORD)
    .set("es.mapping.id", "md5")
    .set("es.mapping.properties.timestamp.type", "date")
    .set("es.mapping.properties.timestamp.format", "date_optional_time")
    .set("es.write.operation", "upsert")
)

spark = (
    pyspark.sql.SparkSession.builder.appName("kapline")
    .config(conf=sparkConf)
    .getOrCreate()
)

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
            response = requests.get(f"http://{HTTPD_HOST}/{filename}").content
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

        # model is trained for only $MAX_RULES rules :(
        features = features[:MAX_RULES]

        # idk if this is the best practice but it works
        return json.dumps(
            {
                "features": [crime_score[1] for crime_score in features],
                "size": report["size_bytes"],
            }
        )

    df = df.withColumn(
        "output", from_json(get_features(df.filename), schema=report_struct)
    ).select("timestamp", "filename", "userid", "md5", "output.*")

    return df


def predict(df: DataFrame, model: PipelineModel) -> DataFrame:
    """
    Uses trained model to predict malware family and send the
    notification to the telegram user as soon as possible.
    """

    @udf
    def send_telegram_notification(userid: int, md5: str, label: str) -> str:
        """
        Send the report to the user
        This function doesn't change any data, returns md5 only to
        trick pyspark lol
        """

        text = f"The file with MD5 `{md5}` was classified as `{label}`"

        if label == "Benign":
            text = f"👍 *No threats found*\n\n{text}."
        else:
            text = f"👎 *Threat found*\n\n{text} malware.\n\n*You shouldn't install this application*, take care."

        data = {
            "chat_id": userid,
            "parse_mode": "MarkdownV2",
            "text": text.replace(".", "\."),
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
            "timestamp",
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


def extract_statistics(df: DataFrame) -> DataFrame:
    """
    Alter the dataframe schema adding some new cols
    based on scores from quark-engine.
    """

    @udf
    def partial_scores(features: list) -> str:
        """
        Total score for one label.
        A label can contain some rules, add up all the scores
        owned by its label
        """
        scores = {}

        for label, value in quark_labels.items():
            score_rule = 0

            for rule in value:

                if rule <= MAX_RULES:
                    score_rule += features[rule - 1]

            scores[f"{label}_score"] = score_rule

        return json.dumps(scores)

    df = df.withColumn(
        "max_score", aggregate("features", lit(0.0), lambda acc, x: acc + x)
    )

    df = df.withColumn(
        "scores",
        from_json(
            partial_scores(df.features),
            schema=label_struct,
        ),
    ).select("timestamp", "md5", "size", "predictedLabel", "max_score", "scores.*")

    return df


def process_message_pointer(
    df: DataFrame, model: PipelineModel = trainedModel
) -> DataFrame:
    """
    First topic
    """
    df = get_message(df, raw_data_struct)
    df = enrich_dataframe(df)
    df = predict(df, model)

    return df


def prepare_for_elastic(df: DataFrame) -> DataFrame:
    """
    Second topic
    """
    df = get_message(df, elastic_struct)
    df = extract_statistics(df)
    df = df.withColumnRenamed("timestamp", "@timestamp")

    return df


"""
In the first topic we want to retrieve the file from
the dataserver and process it to extract features


##########################

Kafka -> Spark -> Kafka

##########################

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
    .option("checkpointLocation", "./analyzed/checkpoint")
    .start()
)

"""
In the second topic we want to enrich the dataframe
with some statistics

The workflow is:

##################################

Kafka -> Spark -> Elastic Search

##################################
"""

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("subscribe", "analyzed")
    .load()
)

df = prepare_for_elastic(df)

query_elastic = (
    df.writeStream.option("checkpointLocation", "./statistics/checkpoints")
    .format("es")
    .start("kaplyzed")
)


query_elastic.awaitTermination()
query_kafka.awaitTermination()
