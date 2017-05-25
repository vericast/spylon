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

    # Create and start a managed progress printer
    pp = sparkprog.start(sc, sleep_time=0.05)
    # Make sure we get an instance of it back
    assert pp

    # Run something simple, and make sure we get a result
    rdd = sc.parallelize(range(2), 2).map(delayed(1))
    reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    assert reduced.map(delayed(1)).collect()

    out, err = capsys.readouterr()
    # Spark spews messages on stderr other than progress when things go wrong.
    # Make sure we're seeing *something* that looks like progress information
    # at least.
    assert "[Stage 0:" in err

    sparkprog.stop()
    # Give the thread time to shutdown
    time.sleep(1)
    # Read whatever was left on the streams
    out, err = capsys.readouterr()

    # Now run something again
    rdd = sc.parallelize(range(4), 2).map(delayed(1))
    reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    assert reduced.map(delayed(1)).collect()

    out, err = capsys.readouterr()
    # We should not see anything after the progress thread is paused.
    assert err == ''


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


def test_convert_array(spark):
    # type: (Spark) -> object
    clist = [1,2,3,4]
    o = spark.jvmh.to_scala_array(clist, "java.lang.Integer")
    # This is the scala type ::
    assert o.getClass().getName() == "[Ljava.lang.Integer;"
    for i, target in enumerate(clist):
        assert o[i] == target


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
