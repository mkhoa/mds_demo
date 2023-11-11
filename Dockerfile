# syntax=docker/dockerfile:1
FROM python:3.10-alpine
FROM mageai/mageai:latest
WORKDIR /code
RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .