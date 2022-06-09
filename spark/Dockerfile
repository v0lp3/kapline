FROM bitnami/spark:latest

USER root

RUN apt update && apt install -y git\
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark kafka-utils requests numpy quark-engine

RUN freshquark

WORKDIR /spark

COPY . .

# ENTRYPOINT [ "spark-submit", "analyze.py"]