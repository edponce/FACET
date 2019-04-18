FROM python:3.7
COPY . /QuickerUMLS
WORKDIR /QuickerUMLS
RUN pip install -e .
