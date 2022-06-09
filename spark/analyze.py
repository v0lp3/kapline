import shutil
import logging
import os

import pyspark
from pyspark.ml import PipelineModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = pyspark.sql.SparkSession.builder.appName("kapline").getOrCreate()

MODEL_DIR = "./model"
KAFKA_HOSTS = os.environ["KAFKA_HOSTS"]

shutil.unpack_archive(filename="model.zip", extract_dir=MODEL_DIR)

trainedModel = PipelineModel.load(MODEL_DIR)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOSTS)
    .option("subscribe", "apk_pointers")
    .load()
    .select("userid", "filename")
)


logger.info(f"TEST: {str(df)}")
