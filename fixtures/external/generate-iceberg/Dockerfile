FROM python:3.11-slim

ARG ICEBERG_VERSION=1.5.2

ENV ICEBERG_VERSION=${ICEBERG_VERSION}
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
  "pyspark==3.5.1" \
  "pyiceberg[pyarrow,sql-sqlite]==0.7.1" \
  "pyarrow==15.0.2"

RUN mkdir -p /opt/iceberg/jars \
  && curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    -o "/opt/iceberg/jars/iceberg-spark-runtime.jar"

WORKDIR /work

COPY generate.py /work/generate.py

ENTRYPOINT ["python", "/work/generate.py"]
