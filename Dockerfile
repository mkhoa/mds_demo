# syntax=docker/dockerfile:1
FROM python:3.10-alpine
FROM mageai/mageai:latest
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .