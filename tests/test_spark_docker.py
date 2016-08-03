from __future__ import print_function, absolute_import
import io
import spylon.spark.launcher as sparklauncher
import spylon.spark.progress as sparkprog
import spylon.spark.utils as sparkutils
import time
import pytest


@pytest.fixture(scope="module")
def spark_tuple_module(request):
    conf = sparklauncher.SparkConfiguration()
    conf.master = "local[1]"
    sc, sqlContext = conf.sql_context("test")
    request.addfinalizer(sc.stop)
    return sc, sqlContext


class Spark:
    def __init__(self, spark_tuple_module):
        self.sc, self.sqlContext = spark_tuple_module
        self.jvmh = sparkutils.SparkJVMHelpers(self.sc)


@pytest.fixture(scope='module')
def spark(spark_tuple_module):
    return Spark(spark_tuple_module)


@pytest.fixture(scope='module')
def sc(spark_tuple_module):
    return spark_tuple_module[0]


def test_progressbar(capsys, sc):
    """Test to ensure that the progressbar handler is working correctly"""

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


@pytest.fixture(
    scope='module',
    params=[
        [1, 2, 3],
        [1.0, 1.1, 1.2],
        ['abc', 'cde', 'def'],
        list(range(100)),
        [1, None, 'a', 99.0]
    ])
def clist(request):
    return request.param


@pytest.fixture(scope='module')
def cmap(request, clist):
    return {k: k for k in clist}


def test_convert_seq(spark, clist):
    # type: (Spark) -> object
    o = spark.jvmh.to_scala_seq(clist)
    assert o.getClass().getName() == "scala.collection.convert.Wrappers$JListWrapper"
    for i, target in enumerate(clist):
        assert o.apply(i) == target


def test_convert_list(spark, clist):
    # type: (Spark) -> object
    o = spark.jvmh.to_scala_list(clist)
    # This is the scala type ::
    assert o.getClass().getName() == "scala.collection.immutable.$colon$colon"
    for i, target in enumerate(clist):
        assert o.apply(i) == target


def test_convert_set(spark, clist):
    # type: (Spark) -> object
    o = spark.jvmh.to_scala_set(set(clist))
    # Scala has different behaviors for very small sets hence two checks here
    java_class_name = o.getClass().getName()
    assert java_class_name.startswith("scala.collection.immutable.Set") or java_class_name.startswith("scala.collection.immutable.HashSet")


def test_convert_map(spark, cmap):
    # type: (Spark) -> object
    o = spark.jvmh.to_scala_map(cmap)
    java_class_name = o.getClass().getName()
    assert java_class_name == 'scala.collection.convert.Wrappers$JMapWrapper'
    for i, target in enumerate(cmap.keys()):
        assert o.apply(target) == target
