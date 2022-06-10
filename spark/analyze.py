import logging
import os
import shutil
import requests

import time

import pyspark
import pyspark.sql.types as tp
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, desc

MODEL_DIR = "./model"
KAFKA_HOSTS = os.environ["KAFKA_HOSTS"]
FILE_HOST = os.environ["FILE_HOST"]
MAX_FEATURES = 204


data_pointer = tp.StructType(
    [
        tp.StructField(name="timestamp", dataType=tp.TimestampType(), nullable=False),
        tp.StructField(name="filename", dataType=tp.StringType(), nullable=False),
        tp.StructField(name="userid", dataType=tp.StringType(), nullable=False),
    ]
)


full_data = tp.StructType(
    [
        tp.StructField(name=f"_{str(i)}", dataType=tp.DoubleType(), nullable=True)
        for i in range(1, MAX_FEATURES + 1)
    ]
)


spark = (
    pyspark.sql.SparkSession.builder.appName("kapline")
    .config("spark.scheduler.mode", "FIFO")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

shutil.unpack_archive(filename="model.zip", extract_dir=MODEL_DIR)

trainedModel = PipelineModel.load(os.path.join(MODEL_DIR, "apkAnalysis"))


def predict(df):

    return df


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("subscribe", "apk_pointers")
    .load()
)

df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json("value", data_pointer).alias("data"))
    .select("data.*")
)

with open("tmp.apk", "wb") as f:
    response = requests.get(f"http://{FILE_HOST}/{df.head().filename}")
    f.write(response.content)


# df = df.applyInPandas(predict, full_data)

df.writeStream.format("console").start().awaitTermination()
