from __future__ import print_function, absolute_import
import io
import spylon.spark.launcher as sparklauncher
import spylon.spark.progress as sparkprog
import time
import pytest


@pytest.fixture(scope="function")
def spark_tuple(request):
    conf = sparklauncher.SparkConfiguration()
    conf.master = "local[1]"
    spark_tuple = conf.sql_context("test")
    def fin():
        sc, _ = spark_tuple
        sc.stop()
    request.addfinalizer(fin)
    return spark_tuple


def test_progressbar(capsys, spark_tuple):
    """Test to ensure that the progressbar handler is working correctly"""
    sc, sqlContext = spark_tuple

    def delayed(seconds):
        def f(x):
            time.sleep(seconds)
            return x
        return f

    sparkprog.start_spark_progress_bar_thread(sc, sleep_time=0.05)

    rdd = sc.parallelize(range(2), 2).map(delayed(1))
    reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    reduced.map(delayed(1)).collect()

    out, err = capsys.readouterr()

    import re
    carriage_returns = re.findall(r'\r', err)
    # Due to timing mismatches we can get a few carriage returns here.
    assert 10 < len(carriage_returns) < 20

    newlines = re.findall(r'\n', err)

    # Due to timing mismatches we can occasionally get 0 here instead of 1.
    assert 0 <= len(newlines) <= 1

    assert '\r[Stage 0:>                    (0 + 0 / 2 Dur' in err
    assert '\r[Stage 0:>                    (0 + 1 / 2 Dur' in err
    # The reduce tasks should take 2 seconds to run
    assert '\r[Stage 1:==========>          (1 + 1 / 2 Dur: 02s]' in err
