from __future__ import print_function, absolute_import
import spylon.spark.launcher as sparklauncher
import os
import sys
import pytest


@pytest.mark.skip(True, reason="Test state leakage not fixed yet")
@pytest.mark.xfail(sys.version_info[0] >= 3, reason="setattr fails on python 3")
def test_set_spark_property():
    c = sparklauncher.SparkConfiguration()
    c.driver_memory = "4g"


def test_sparkconf_hasattr():
    c = sparklauncher.SparkConfiguration()
    assert hasattr(c, "foo") is False
    assert hasattr(c, "driver_memory") is True
    assert hasattr(c, "driver-memory") is True
    assert c.driver_memory is None
    c.driver_memory = "4g"
    assert c.driver_memory == "4g"
    with pytest.raises(AttributeError):
        # make sure that attempting to access unknown variables raises an attribute error
        getattr(c, 'foo')
    with pytest.raises(AttributeError):
        # make sure that attempting to set unknown variables raises an attribute error
        c.foo = "bar"
        
                
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


@pytest.mark.xfail(True, reason="Config parameter priority not sorted out yet")
def test_config_priority():
    c = sparklauncher.SparkConfiguration()
    c.driver_memory = "4g"
    c.conf.spark.driver.memory = "5g"
    c._set_environment_variables()
    assert '--driver-memory 5g' in os.environ['PYSPARK_SUBMIT_ARGS']
