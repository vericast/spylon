import spylon.spark.launcher as sparklauncher
import os

def test_set_spark_property():
    c = sparklauncher.SparkConfiguration()
    c.driver_memory = "4g"

def test_spark_driver_memory():
    c = sparklauncher.SparkConfiguration()
    c.conf.spark.driver.memory = "5g"
    c._set_environment_variables()
    assert '--driver-memory 5g' in os.environ['PYSPARK_SUBMIT_ARGS']
