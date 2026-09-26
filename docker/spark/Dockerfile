FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && mkdir -p /workspace/notebooks /workspace/data /workspace/output /opt/project \
    && chown -R spark:spark /workspace /opt/project

COPY docker/hadoop/core-site.xml /opt/spark/conf/core-site.xml
COPY docker/hadoop/hdfs-site.xml /opt/spark/conf/hdfs-site.xml
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/start-jupyter.sh /opt/project/start-jupyter.sh

RUN chmod 0755 /opt/project/start-jupyter.sh \
    && chown -R spark:spark /opt/project /opt/spark/conf

USER spark
WORKDIR /workspace

ENV HADOOP_CONF_DIR=/opt/spark/conf \
    HOME=/workspace \
    JUPYTER_RUNTIME_DIR=/tmp/jupyter-runtime \
    PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
