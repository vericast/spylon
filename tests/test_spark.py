import spylon.spark.progress
import pyspark.status
import datetime
import spylon.spark.launcher as sparklauncher


def test_progressbar_formatter():
    mock_stage_info = pyspark.status.SparkStageInfo(
        stageId=4, currentAttemptId=1, name="test", numTasks=100, numActiveTasks=10,
        numCompletedTasks=20, numFailedTasks=5)
    duration = datetime.timedelta(days=1, hours=1, minutes=1, seconds=1)

    a = spylon.spark.progress._format_stage_info(bar_width=10, stage_info=mock_stage_info, duration=duration)

    assert a == '[Stage 4:==>        (20 + 10 / 100 Dur: 1d01h01m:01s]'


def test_spark_property():

    c = sparklauncher.SparkConfiguration()
    c.conf.spark.executor.cores = 5

    assert c.conf._conf_dict.get('spark.executor.cores') == 5

    c.conf.set_if_unset('spark.executor.cores', 10)

    assert c.conf._conf_dict.get('spark.executor.cores') == 5
