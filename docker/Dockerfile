FROM python:3.7

WORKDIR /FACET
COPY . .

# Install FACET
RUN pip install .

# Install spaCy language support
RUN python -m spacy download en
#
# Install NLTK NLP
RUN python scripts/setup_nltk.py

# Install Simstring terms
# RUN ["./scripts/wait-for-it.sh", "redis:6379", "--", "python", "facet/scripts/cli.py", "run", "-c", "config/facet_docker.yaml:Install"]

# Start FACET server
EXPOSE 4444
# CMD ["./scripts/wait-for-it.sh", "redis:6379", "--", "python", "facet/scripts/cli.py", "server", "-c", "config/facet_docker.yaml:Search"]
