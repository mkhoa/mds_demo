FROM python:3.11-bullseye
LABEL description="Mage data management platform"
ARG PIP=pip3
USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update -y && apt-get install -y openjdk-11-jdk
RUN pip install --upgrade pip

COPY application/mage_ai/requirements.txt /tmp/requirements.txt
RUN \
  pip3 install -r /tmp/requirements.txt && \
  rm /tmp/requirements.txt

RUN pip install mage-ai

ENV PYTHONPATH="${PYTHONPATH}:/home/src"
WORKDIR /home/src

# FROM mageai/mageai:latest
# # Install OpenJDK 11
# ## Add Debian Bullseye repository
# RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list
# RUN apt-get update -y && apt-get install -y openjdk-11-jdk
# ## Remove Debian Bullseye repository
# RUN rm /etc/apt/sources.list.d/bullseye.list
# # Install Requirements
# RUN apt-get update
# RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# RUN apt-get --fix-broken -y install ./google-chrome-stable_current_amd64.deb
# COPY application/mage_ai/requirements.txt /tmp/requirements.txt
# RUN pip install --upgrade pip
# RUN pip install -r /tmp/requirements.txt