FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y gettext-base && \
    rm -rf /var/lib/apt/lists/*

# Versioning
ARG SPARK_MAJOR_VERSION="3.5"
ARG ICEBERG_VERSION="1.5.2"
ARG HADOOP_VERSION="3.3.4"
ARG AWS_SDK_VERSION="1.12.262"
ARG ORACLE_JDBC_VERSION="21.19.0.0"
ARG SPARK_HOME=/opt/spark

WORKDIR /opt/spark/jars

RUN curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${ORACLE_JDBC_VERSION}/ojdbc11-${ORACLE_JDBC_VERSION}.jar && \
    mkdir -p /home/iceberg/localwarehouse /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg

WORKDIR ${SPARK_HOME}


COPY spark-defaults.conf.template /opt/spark/conf/
COPY spark.entrypoint.sh entrypoint.sh

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}" \
HOME="/home/spark"

RUN mkdir -p /home/spark && \
    usermod -d /home/spark spark && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark/conf && \
    chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/* && \
    chmod u+x /opt/spark/entrypoint.sh

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]