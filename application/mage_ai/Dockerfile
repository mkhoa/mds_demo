# syntax=docker/dockerfile:1
FROM mageai/mageai:latest
COPY application/mage_ai/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt
COPY . .