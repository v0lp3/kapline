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


"""
raw_data_struct is a struct that contains raw data. The filename is used as url to download the
file and process it.
"""

raw_data_struct = tp.StructType(
    [
        tp.StructField(name="timestamp", dataType=tp.TimestampType(), nullable=False),
        tp.StructField(name="filename", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="userid", dataType=tp.LongType(), nullable=False),
        tp.StructField(name="md5", dataType=tp.StringType(), nullable=False),
    ]
)


"""
report_struct is a struct that contains some informations that
we extract from the quark-engine report. 
"""

report_struct = tp.StructType(
    [
        tp.StructField(
            name="features", dataType=tp.ArrayType(tp.DoubleType()), nullable=False
        ),
        tp.StructField(name="size", dataType=tp.LongType(), nullable=False),
    ]
)


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
    ).select("timestamp", "filename", "userid", "md5", "output.*")

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

    df = df.withColumn(
        "md5", send_telegram_notification(df.userid, df.md5, df.predictedLabel)
    ).select(
        "timestamp", "filename", "userid", "md5", "features", "size", "predictedLabel"
    )

    return df


def get_message(df: DataFrame) -> DataFrame:
    return (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema=raw_data_struct).alias("data"))
        .select("data.*")
    )


def process_message_pointer(df: DataFrame, model: PipelineModel = trainedModel):
    df = get_message(df)
    df = enrich_dataframe(df)
    df = predict(df, model)

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

query = (
    df.select(to_json(struct("*")).alias("value"))
    .selectExpr("CAST(value AS STRING)")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("topic", "analyzed")
    .option("checkpointLocation", "./checkpoints-api")
    .start()
)

query.awaitTermination()
