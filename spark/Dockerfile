FROM bitnami/spark:latest

USER root

RUN apt update && apt install -y git gpg\
    && rm -rf /var/lib/apt/lists/*

RUN echo 'deb http://download.opensuse.org/repositories/home:/RizinOrg/Debian_11/ /' | tee /etc/apt/sources.list.d/home:RizinOrg.list \
    && curl -fsSL https://download.opensuse.org/repositories/home:RizinOrg/Debian_11/Release.key | gpg --dearmor | tee /etc/apt/trusted.gpg.d/home_RizinOrg.gpg > /dev/null \
    && apt update \
    && apt install -y rizin

RUN pip3 install pyspark kafka-utils requests numpy quark-engine

RUN freshquark

WORKDIR /spark

COPY . .

ENTRYPOINT [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.2.2", "analyze.py"]