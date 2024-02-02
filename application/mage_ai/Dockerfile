# syntax=docker/dockerfile:1
FROM mageai/mageai:latest
RUN apt-get update
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get --fix-broken -y install ./google-chrome-stable_current_amd64.deb
COPY application/mage_ai/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt
COPY . .