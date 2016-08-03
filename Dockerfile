FROM jupyter/pyspark-notebook

USER root

RUN conda install --yes pytest coverage

RUN conda install --yes -c conda-forge findspark

COPY . /repo

WORKDIR /repo

RUN python setup.py install

CMD coverage run -m pytest tests -vrsx --capture=sys && coverage report -m
