# syntax=docker/dockerfile:1
FROM jupyter/scipy-notebook:latest
USER root 
RUN sudo apt-get update
RUN sudo apt-get -y upgrade
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get --fix-broken -y install ./google-chrome-stable_current_amd64.deb
USER jovyan
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
COPY application/jupyterlab/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN python -m pip install psutil
RUN pip install -r /tmp/requirements.txt
EXPOSE 8888
CMD jupyter lab --ip=* --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab --notebook-dir=/opt/app/data --NotebookApp.token='admin' --NotebookApp.allow_origin='*'
COPY . .