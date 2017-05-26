from __future__ import print_function, absolute_import
import datetime
import io
import time

import pyspark.status
import pytest
import spylon.spark.launcher as sparklauncher
import spylon.spark.progress as sparkprog
import spylon.spark.utils as sparkutils


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


def test_progressbar_formatter():
    """Formatter should turn SparkStageInfo into expected progress strings."""
    mock_stage_info = pyspark.status.SparkStageInfo(
        stageId=4, currentAttemptId=1, name="test", numTasks=100, numActiveTasks=10,
        numCompletedTasks=20, numFailedTasks=5)
    duration = datetime.timedelta(days=1, hours=1, minutes=1, seconds=1)

    a = sparkprog._format_stage_info(bar_width=10, stage_info=mock_stage_info, duration=duration)

    assert a == '[Stage 4:==>        (20 + 10 / 100 Dur: 1d01h01m:01s]'


def test_progressbar(capsys, sc):
    """Progress bar should start, pause, resume, and stop properly."""
    def delayed(seconds):
        def f(x):
            time.sleep(seconds)
            return x
        return f

    def run_job():
        """Simple Spark job that should trigger progress bars."""
        rdd = sc.parallelize(range(2), 2).map(delayed(1))
        reduced = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        return reduced.map(delayed(1)).collect()

    # Create and start a managed progress printer
    pp = sparkprog.start(sc, sleep_time=0.05)
    # Make sure we get an instance of it back
    assert pp

    # Run something simple, and make sure we get a result
    assert run_job()

    # Spark spews messages on stderr other than progress when things go wrong.
    # Make sure we're seeing *something* that looks like progress information
    # at least.
    out, err = capsys.readouterr()
    assert "[Stage 0:" in err

    # Pause, give the thread time to sleep, and empty the streams
    pp.pause()
    time.sleep(1)
    out, err = capsys.readouterr()

    # Run again, make sure we see nothing
    assert run_job()
    out, err = capsys.readouterr()
    assert err == ''

    # Resume, give the thread time to wake up, and empty the streams
    pp.resume()
    time.sleep(1)
    out, err = capsys.readouterr()

    # Run again, make sure we see something starting at stage 4
    # since we've run through stages 0-3 so far.
    assert run_job()
    out, err = capsys.readouterr()
    assert "[Stage 4:" in err

    # Stop, give the thread time to shutdown, clear the streams
    sparkprog.stop()
    time.sleep(1)
    out, err = capsys.readouterr()

    # Now run something again, and make sure we see nothing
    assert run_job()
    out, err = capsys.readouterr()
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
