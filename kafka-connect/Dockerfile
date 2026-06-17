FROM quay.io/debezium/connect:2.7.3.Final

USER root

ENV CONFLUENT_VERSION=7.7.1

RUN curl -fL -o /tmp/avro-converter.zip \
        "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/$CONFLUENT_VERSION/confluentinc-kafka-connect-avro-converter-$CONFLUENT_VERSION.zip" && \
    unzip -q /tmp/avro-converter.zip -d /kafka/connect/ && \
    rm /tmp/avro-converter.zip

    # Elasticsearch Sink Connector từ Confluent Hub
    # Stream Kafka topic → ES indices, hỗ trợ SMT unwrap Debezium envelope
ARG ES_SINK_VERSION=14.1.2
RUN curl -L -o /tmp/es-sink.zip \
        https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/${ES_SINK_VERSION}/confluentinc-kafka-connect-elasticsearch-${ES_SINK_VERSION}.zip \
    && unzip -q /tmp/es-sink.zip -d /kafka/connect/ \
    && rm /tmp/es-sink.zip


    # Confluent S3 Sink Connector — stream Kafka → S3/MinIO Parquet
    # Output: Hive-style partitioned files (year/month/day/hour)
ARG S3_SINK_VERSION=10.5.13
RUN curl -L -o /tmp/s3-sink.zip \
        https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/${S3_SINK_VERSION}/confluentinc-kafka-connect-s3-${S3_SINK_VERSION}.zip \
    && unzip -q /tmp/s3-sink.zip -d /kafka/connect/ \
    && rm /tmp/s3-sink.zip

USER 1001