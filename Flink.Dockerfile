FROM flink:1.20.5-scala_2.12-java17
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
COPY submit.sh pipeline.sql /opt/flink/


