# https://github.com/docker-library/redis/blob/master/5.0/Dockerfile
# https://medium.com/commencis/creating-redis-cluster-using-docker-67f65545796d
# Download image:
# > docker pull redis:3.2
FROM python:3.7

WORKDIR /FACET

# RUN apt -qq update
RUN apt update
RUN apt upgrade
RUN apt install -y build-essential
RUN apt install -y redis-server

# Install pip
RUN python -m pip install --upgrade pip

# Install Conda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN bash Miniconda3-latest-Linux-x86_64.sh

# Install FACET Python package
COPY . /FACET

# Set up a conda environment
# RUN conda env create -n facet python=3.7
# RUN conda env create --file /FACET/environment.yml
# RUN conda activate facet

RUN pip install /FACET

# Install spaCy language support
RUN python -m spacy download en

# Install NLTK NLP
RUN python /FACET/scripts/setup_nltk.py

# Set up FACET command shell completion
RUN source /FACET/scripts/shell_completion.sh

# Start Redis server
EXPOSE 6379
RUN systemctl enable redis-server

# Start FACET server
EXPOSE 4444
RUN facet server -c /FACET/config/factory.yaml:SimpleRedis -p 4444
