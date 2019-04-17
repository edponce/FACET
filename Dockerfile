FROM rayproject/examples

COPY . /quickerumls

WORKDIR /quickerumls
RUN pip install -e .
