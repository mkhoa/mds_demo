# syntax=docker/dockerfile:1
FROM python:3.10-alpine
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
COPY requirements.txt requirements.txt
RUN apk add build-base linux-headers
RUN apk add --update gcc
RUN pip install --upgrade pip
RUN python -m pip install psutil
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .