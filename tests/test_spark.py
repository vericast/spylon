import spylon.spark.progress
import pyspark.status
import datetime


def test_progressbar_formatter():
    mock_stage_info = pyspark.status.SparkStageInfo(
        stageId=4, currentAttemptId=1, name="test", numTasks=100, numActiveTasks=10,
        numCompletedTasks=20, numFailedTasks=5)
    duration = datetime.timedelta(days=1, hours=1, minutes=1, seconds=1)

    a = spylon.spark.progress._format_stage_info(bar_width=10, stage_info=mock_stage_info, duration=duration)

    assert a == '[Stage 4:==>        (20 + 10 / 100 Dur: 1d01h01m:01s]'


