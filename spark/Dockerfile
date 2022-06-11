FROM bitnami/spark:latest

USER root

RUN apt update && apt install -y git\
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark kafka-utils requests numpy quark-engine

RUN freshquark

WORKDIR /spark

COPY . .

ENTRYPOINT [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.2.2", "analyze.py"]