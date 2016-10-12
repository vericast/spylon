# Copyright (c) 2016 MaxPoint Interactive, Inc.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#    disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
#    products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from __future__ import print_function, absolute_import, division

from collections import defaultdict
import sys
import datetime
import time
import threading


def _pretty_time_delta(td):
    """A representation for timedelta

    Parameters
    ----------
    td : :class:`datetime.timedelta`

    Returns
    -------
    pretty_formatted_datetime : str

    """
    seconds = td.total_seconds()
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    d = dict(sign=sign_string, days=days, hours=hours, minutes=minutes, seconds=seconds)
    if days > 0:
        return '{sign}{days}d{hours:02d}h{minutes:02d}m:{seconds:02d}s'.format(**d)
    elif hours > 0:
        return '{sign}{hours:02d}h{minutes:02d}m:{seconds:02d}s'.format(**d)
    elif minutes > 0:
        return '{sign}{minutes:02d}m:{seconds:02d}s'.format(**d)
    else:
        return '{sign}{seconds:02d}s'.format(**d)


def _spark_progress_thread_worker(sc, timedelta_formatter=_pretty_time_delta, bar_width=20, sleep_time=0.5):
    """Non terminating process used inside the progress bar thread

    Parameters
    ----------
    sc: :class:`pyspark.context.SparkContext`
        Spark context to use.
    timedelta_formatter : function
        Callable that converts a timedelta to a string.
    bar_width : int
        Width of the progressbar to print out.
    sleep_time : float
        Frequency in seconds with which to poll Apache Spark for task stage information.

    """
    global _progressbar_thread_started
    last_status = ''
    start_times = defaultdict(datetime.datetime.now)
    max_stage_id = -1

    status = sc.statusTracker()
    while True:
        # SparkContext has been stopped, terminate loop.
        if sc._jsc is None:
            break
        stage_ids = status.getActiveStageIds()
        progressbar_list = []
        # Only show first 3
        stage_counter = 0
        current_max_stage = max_stage_id
        for stage_id in stage_ids:
            stage_info = status.getStageInfo(stage_id)
            if stage_info and stage_info.numTasks > 0:
                # Set state variables used for flushing later
                current_max_stage = stage_id
                stage_counter += 1
                td = datetime.datetime.now() - start_times[stage_id]
                s = _format_stage_info(bar_width, stage_info, td, timedelta_formatter)
                progressbar_list.append(s)
            if stage_counter == 3:
                break

        # Ensure that when we get a new maximum stage id we print a \n to make the progress bar go on to the next
        # line.
        if current_max_stage > max_stage_id:
            if last_status != '':
                sys.stderr.write("\n")
            sys.stderr.flush()
            max_stage_id = current_max_stage

        new_status = ' '.join(progressbar_list)
        if new_status != last_status:
            sys.stderr.write("\r" + new_status)
            sys.stderr.flush()
            last_status = new_status
        time.sleep(sleep_time)

    # unset the progress bar global.
    _progressbar_thread_started = False


def _format_stage_info(bar_width, stage_info, duration, timedelta_formatter=_pretty_time_delta):
    """

    Parameters
    ----------
    bar_width : int
    stage_info : :class:`pyspark.status.StageInfo`
    stage_id : int
    duration : :class:`datetime.timedelta`
    timedelta_formatter : callable

    Returns
    -------
    formatted : str
    """
    dur = timedelta_formatter(duration)
    percent = (stage_info.numCompletedTasks * bar_width) // stage_info.numTasks
    bar = [' '] * bar_width
    for i in range(bar_width):
        char = ' '
        if i < percent:
            char = '='
        if i == percent:
            char = '>'
        bar[i] = char
    bar = ''.join(bar)
    return "[Stage {info.stageId}:{bar} " \
           "({info.numCompletedTasks} + {info.numActiveTasks} / {info.numTasks} Dur: {dur}]" \
        .format(info=stage_info, dur=dur, bar=bar)


_progressbar_thread_started = False


def start_spark_progress_bar_thread(sc, **kwargs):
    """Starts a background thread that polls the spark context for progress on stages that are running.

    This makes it simple to have progress bars for spark jobs inside Jupyter notebooks.

    Parameters
    ----------
    sc : :class:`SparkContext`
    """
    global _progressbar_thread_started

    if _progressbar_thread_started:
        raise RuntimeError("Spark progress thread already running")

    t = threading.Thread(target=_spark_progress_thread_worker, args=[sc], kwargs=kwargs)
    t.start()
    _progressbar_thread_started = True
