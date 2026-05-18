FROM spark:4.1.1-scala2.13-java17-ubuntu

USER root

RUN set -ex; \
    echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main" > /etc/apt/sources.list.d/deadsnakes-ubuntu-ppa-jammy.list; \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6A755776; \
    apt-get update; \
    apt-get install -y curl python3.12 python3.12-dev python3.12-venv; \
    ln -sf /usr/bin/python3.12 /usr/bin/python3; \
    curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py; \
    python3.12 /tmp/get-pip.py; \
    python3.12 -m pip install --no-cache-dir boto3 Pillow pymilvus==2.5.4 pyspark pyarrow deltalake; \
    python3.12 -m pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch; \
    python3.12 -m pip install --no-cache-dir transformers; \
    rm -rf /var/lib/apt/lists/* /tmp/get-pip.py

ENV PYSPARK_PYTHON=/usr/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

USER spark
