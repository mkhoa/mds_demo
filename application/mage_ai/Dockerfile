FROM mageai/mageai:latest
LABEL description="Mage data management platform"
USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update -y && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

COPY application/mage_ai/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/home/src"
WORKDIR /home/src
