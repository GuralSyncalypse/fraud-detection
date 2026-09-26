FROM apache/hadoop:3.4.3

USER root

COPY docker/hadoop/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY docker/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY docker/hadoop/start-namenode.sh /opt/project/start-namenode.sh
COPY docker/hadoop/start-datanode.sh /opt/project/start-datanode.sh
COPY docker/hadoop/upload-raw.sh /opt/project/upload-raw.sh

RUN mkdir -p /data/name /data/data /opt/project \
    && chmod 0755 /opt/project/start-namenode.sh /opt/project/start-datanode.sh /opt/project/upload-raw.sh \
    && chown -R hadoop:hadoop /data /opt/project /opt/hadoop/etc/hadoop

USER hadoop

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
