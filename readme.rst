Spylon
======

|Version_Status| |Travis| |Conda_Forge| |Docs|

A set of compatibility routines for making it easier to interact with Scala from
Python.

Occasionally Python-focused data shops need to use JVM languages for performance
reasons. Generally this necessitates throwing away whole repositories of Python
code and starting over or resorting to service architectures (e.g., Apache
thrift) which increase system complexity.

You don't have to.

Using py4j and Spylon you can readily interact with Scala code for more
performance critical sections of your code whilst leaving the rest unmodified.

Alternatively you can use it as a bridge to allow building wrappers for a
Scala/Java codebase.

Installation
------------
Spylon can be installed either from pip or conda-forge.

When installing from pip and the desire is to use it with Apache Spark, you should run

```
pip install spylon[spark]
```

Usage
-----
The simplest way to use spylon is to use it to help with writing PySpark jobs.
If you want to supply your own jars to load for usage as Spark user defined
functions, you'd want to supply the jar with the udf implementation to spark via
spark-submit.

For an easier interactive experience you can make use of the supplied Apache
Spark launcher to make it simpler to instantiate a PySpark application from
inside a python Jupyter notebook.

Extensions
----------
Spylon is designed as an easy to extend toolkit.  Since Apache Spark is a major
user or Py4J, some special use cases have been implemented for that and its an
example of some use cases for Spylon.


.. |Version_Status| image:: https://img.shields.io/pypi/v/spylon.svg
   :target: https://pypi.python.org/pypi/spylon/
.. |Downloads| image:: https://img.shields.io/pypi/dm/spylon.svg
   :target: https://pypi.python.org/pypi/spylon/
.. |Conda_Forge| image:: https://anaconda.org/conda-forge/spylon/badges/version.svg
   :target: https://anaconda.org/conda-forge/spylon
.. |Travis| image:: https://travis-ci.org/maxpoint/spylon.svg
   :target: https://travis-ci.org/maxpoint/spylon
.. |Docs| image:: https://img.shields.io/badge/docs-github-brightgreen.svg
   :target: https://maxpoint.github.io/spylon/
