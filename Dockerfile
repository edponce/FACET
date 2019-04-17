FROM python:3.7

COPY . /quickerumls

WORKDIR /quickerumls

RUN pip install -e .
