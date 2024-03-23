# syntax=docker/dockerfile:1
FROM mageai/mageai:latest
# Install OpenJDK 11
## Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list
RUN apt-get update -y && apt-get install -y openjdk-11-jdk
## Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list
# Install Requirements
RUN apt-get update
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get --fix-broken -y install ./google-chrome-stable_current_amd64.deb
COPY application/mage_ai/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt