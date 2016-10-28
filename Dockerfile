FROM jupyter/pyspark-notebook:54838ed4acb1

USER root

RUN apt-get update && apt-get install --yes zip

# test dependencies
RUN conda install --yes pytest coverage

# runtime dependencies
RUN conda install --yes -c conda-forge findspark pandas pyyaml

# sphinx doc build dependencies
RUN conda install --yes -c conda-forge sphinx numpydoc sphinx_rtd_theme

COPY . /repo

WORKDIR /repo

RUN python setup.py install

RUN pip install py4j==0.9

CMD coverage run run_tests.py && coverage report -m && cp .coverage /repo
