FROM apache/airflow:3.1.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && mkdir -p /opt/java \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java/openjdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
        "deltalake" \
        "polars" \
        "pyarrow" \
        "duckdb" \
        "pandas" \
        "boto3" \
        "minio" \
        "kafka-python==2.3.0" \
        "Pillow" \
        "pyspark==4.1.1" \
        "pymilvus==2.5.4" \
        "fastapi" \
        "uvicorn[standard]" \
        "requests"
