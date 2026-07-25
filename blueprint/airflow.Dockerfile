FROM apache/airflow:slim-3.3.0-python3.12

USER root

# Install OpenJDK 17 specifically (matching the Spark container) and wget
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         wget \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Download pre-packaged Spark with Hadoop 3 support
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
ARG SPARK_MAJOR_VERSION="3.5"
ARG ICEBERG_VERSION="1.5.2"
ARG HADOOP_VERSION="3.3.4"
ARG AWS_SDK_VERSION="1.12.262"
ARG ORACLE_JDBC_VERSION="21.19.0.0"

RUN curl -fsSLO https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && chown -R airflow: /opt/spark \
    && chmod +x /opt/spark/bin/* \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

WORKDIR /opt/spark/jars

RUN curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${ORACLE_JDBC_VERSION}/ojdbc11-${ORACLE_JDBC_VERSION}.jar

    
WORKDIR /opt/airflow
    
USER airflow 

COPY spark-defaults.conf /opt/spark/conf/ 
COPY ./airflow.requirements.txt .

RUN pip install --no-cache-dir -r ./airflow.requirements.txt