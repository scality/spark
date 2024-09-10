FROM python:3.8-slim-bullseye

RUN --mount=type=cache,target=/var/cache/apt apt update \
    && apt-get install -y --no-install-recommends \
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

# Optional env variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV SPARK_MASTER_HOST="spark-master"
ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

RUN mkdir -p ${HADOOP_HOME} ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

COPY requirements.txt /tmp/requirements.txt
COPY scality-0.1-py3-none-any.whl /tmp/
COPY --from=ghcr.io/astral-sh/uv:0.4.8 /uv /bin/uv

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip compile /tmp/requirements.txt > /tmp/requirements-compiled.txt \
    && uv pip sync --system /tmp/requirements-compiled.txt \
    && uv pip install --system /tmp/scality-0.1-py3-none-any.whl

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

# globbing to not fail if not found
COPY spark-3.5.2-bin-hadoop3.tg[z] /tmp/
# -N enable timestamping to condition download if already present or not
RUN cd /tmp \
    && wget -N https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.2-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -f spark-3.5.2-bin-hadoop3.tgz

# https://github.com/sayedabdallah/Read-Write-AWS-S3
COPY aws-java-sdk-1.12.770.ja[r] /spark/jars/
COPY hadoop-aws-3.3.4.ja[r] /spark/jars/
RUN cd /spark/jars/ \
    && wget -N https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.770/aws-java-sdk-1.12.770.jar \
    && wget -N https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN chmod u+x /opt/spark/sbin/* /opt/spark/bin/*

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]