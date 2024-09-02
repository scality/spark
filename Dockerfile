FROM python:3.8-slim-bullseye as spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    wget \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk \
    build-essential \
    software-properties-common \
    ssh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# globbing to not fail if not found
COPY spark-3.5.2-bin-hadoop3.tg[z] /tmp/
# -N enable timestamping to condition download if already present or not
RUN cd /tmp \
    && wget -N https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.2-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -f spark-3.5.2-bin-hadoop3.tgz

FROM spark-base as pyspark

# Install python deps
RUN --mount=type=cache,target=/root/.cache/pip pip install \
    pandas \
    numpy \
    requests \
    pyyaml \
    pyopenssl \
    certifi \
    pycurl \
    kazoo \
    pyzmq \
    python-dateutil \
    s3fs \
    pyspark==3.5.2

COPY scality-0.1-py3-none-any.whl /tmp/
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-index file:///tmp/scality-0.1-py3-none-any.whl

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

# https://github.com/sayedabdallah/Read-Write-AWS-S3
COPY aws-java-sdk-1.12.770.ja[r] /spark/jars/
COPY hadoop-aws-3.3.4.ja[r] /spark/jars/
RUN cd /spark/jars/ \
    && wget -N https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.770/aws-java-sdk-1.12.770.jar \
    && wget -N https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN chmod u+x /opt/spark/sbin/* /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]