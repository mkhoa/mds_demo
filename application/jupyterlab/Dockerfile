# syntax=docker/dockerfile:1
FROM jupyter/scipy-notebook:latest
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
COPY application/jupyterlab/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN python -m pip install psutil
RUN pip install -r /tmp/requirements.txt
EXPOSE 8888
CMD jupyter lab --ip=* --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab --notebook-dir=/opt/app/data --NotebookApp.token='admin' --NotebookApp.allow_origin='*'
COPY . .