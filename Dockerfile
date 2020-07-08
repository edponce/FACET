# https://github.com/docker-library/redis/blob/master/5.0/Dockerfile
# https://medium.com/commencis/creating-redis-cluster-using-docker-67f65545796d
# docker pull redis
FROM python:3.7

WORKDIR /FACET

# RUN apt -qq update
RUN apt update
RUN apt upgrade
RUN apt install -y build-essential
RUN apt install -y redis-server

# Start Redis server
EXPOSE 6379
RUN systemctl enable redis-server

# Install FACET Python package
COPY . /FACET
RUN conda env create --file /FACET/environment.yml
RUN echo 'source activate facet' >> ~/.bashrc

# Install spaCy data
RUN sh scripts/setup_spacy.sh

# Install NLTK data
RUN python scripts/setup_nltk.py

# Set up FACET command autocompletion
RUN source /FACET/scripts/autocompletion.sh

# Start FACET server
EXPOSE 4444
RUN facet server -c config/factory.yaml:SimpleMemory
