FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1


RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

RUN mkdir -p /opt/flink/jobs