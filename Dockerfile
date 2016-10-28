FROM jupyter/pyspark-notebook:54838ed4acb1

USER root

RUN apt-get update && apt-get install --yes zip

# test dependencies
RUN conda install --yes pytest coverage

# runtime dependencies
RUN conda install --yes -c conda-forge findspark pandas pyyaml

# sphinx doc build dependencies
RUN conda install --yes -c conda-forge sphinx numpydoc sphinx_rtd_theme

COPY . /repo-copy

WORKDIR /repo-copy

# Need to install the python package so that the docs building works.
# docs/conf.py has an `import spylon` line
RUN pip install .

RUN pip install py4j==0.9

CMD coverage run run_tests.py && coverage report -m && cp .coverage /repo
