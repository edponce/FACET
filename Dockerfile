# https://github.com/docker-library/redis/blob/master/5.0/Dockerfile
# https://medium.com/commencis/creating-redis-cluster-using-docker-67f65545796d
# docker pull redis
FROM python:3.7

WORKDIR /QuickerUMLS

# RUN apt -qq update
RUN apt update
RUN apt upgrade
RUN apt install -y build-essential
RUN apt install -y redis-server

# Start Redis server
EXPOSE 6379
RUN systemctl enable redis-server

COPY . /QuickerUMLS
RUN conda env create --file /QuickerUMLS/environment.yml
RUN echo 'source activate quickerumls' > ~/.bashrc
