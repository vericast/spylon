FROM jupyter/pyspark-notebook:54838ed4acb1

USER root

RUN conda install --yes pytest coverage

RUN conda install --yes -c conda-forge findspark

COPY . /repo

WORKDIR /repo

RUN python setup.py install

RUN pip install py4j==0.9

CMD coverage run -m pytest tests -vrsx --capture=sys --color=yes && coverage report -m
