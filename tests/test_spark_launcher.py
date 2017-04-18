from __future__ import print_function, absolute_import
import spylon.spark.launcher as sparklauncher
import os

__author__ = 'mniekerk'


def test_sparkconf_hasattr():
    c = sparklauncher.SparkConfiguration()
    assert hasattr(c, "foo") is False

def test_spark_property():
    c = sparklauncher.SparkConfiguration()
    c.conf.spark.executor.cores = 5
    assert c.conf._conf_dict.get('spark.executor.cores') == 5

    c.conf.set_if_unset('spark.executor.cores', 10)
    assert c.conf._conf_dict.get('spark.executor.cores') == 5


def test_spark_launcher_argument():
    c = sparklauncher.SparkConfiguration()
    archive = "/path/to/some/archive.zip"
    c.archives = archive
    c._set_environment_variables()
    assert ('--archives ' + archive) in os.environ['PYSPARK_SUBMIT_ARGS']


def test_spark_launcher_multiple_argument():
    c = sparklauncher.SparkConfiguration()
    archive = ["/path/to/some/archive.zip", "/path/to/someother/archive.zip"]
    c.archives = archive
    c._set_environment_variables()
    assert ('--archives ' + ','.join(archive)) in os.environ['PYSPARK_SUBMIT_ARGS']


def test_spark_driver_memory():
    c = sparklauncher.SparkConfiguration()
    c.conf.spark.driver.memory = "5g"
    c._set_environment_variables()
    assert '--driver-memory 5g' in os.environ['PYSPARK_SUBMIT_ARGS']
