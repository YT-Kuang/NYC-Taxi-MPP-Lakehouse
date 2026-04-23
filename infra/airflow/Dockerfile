FROM apache/airflow:2.11.0-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    tini \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN JAVA_DIR=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -s ${JAVA_DIR} /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

COPY infra/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt