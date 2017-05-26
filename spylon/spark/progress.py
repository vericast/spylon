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
    """Creates a string representation of a time delta.

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


def _format_stage_info(bar_width, stage_info, duration, timedelta_formatter=_pretty_time_delta):
    """Formats the Spark stage progress.

    Parameters
    ----------
    bar_width : int
        Width of the progressbar to print out.
    stage_info : :class:`pyspark.status.StageInfo`
        Information about the running stage
    stage_id : int
        Unique ID of the stage
    duration : :class:`datetime.timedelta`
        Duration of the stage so far
    timedelta_formatter : callable
        Converts a timedelta to a string.

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


class ProgressPrinter(threading.Thread):
    """Polls a Spark StatusTracker for information about active stages and writes
    stage progress to stderr.

    Attributes
    ----------
    sc: :class:`pyspark.context.SparkContext`
        Spark context to monitor
    timedelta_formatter : callable
        Callable that converts a timedelta to a string.
    bar_width : int
        Width of the progressbar to print out.
    sleep_time : float
        Frequency in seconds with which to poll Apache Spark for task stage information.
    condition: threading.Condition
        Condition on which the thread should sleep or awake
    daemon: bool
        True to daemonize the thread to avoid keeping the main process alive
    paused: bool
        True when paused, false when resumed
    alive: bool
        True when alive, false when the thread should end
    """
    def __init__(self, sc, timedelta_formatter, bar_width, sleep_time):
        threading.Thread.__init__(self)
        self.condition = threading.Condition()
        self.sc = sc
        self.timedelta_formatter = timedelta_formatter
        self.bar_width = bar_width
        self.sleep_time = sleep_time
        self.daemon = True
        self.paused = False
        self.alive = True

    def __del__(self):
        """Cleanup when destroyed."""
        self.alive = False

    def shutdown(self):
        """Stop all progress updates and end the thread."""
        with self.condition:
            self.alive = False

    def pause(self):
        """Pause progress updates."""
        with self.condition:
            self.paused = True

    def resume(self):
        """Resume progress updates."""
        with self.condition:
            self.paused = False
            self.condition.notify_all()

    def run(self):
        """Run the progress printing loop."""
        last_status = ''
        # lambda is used to avoid http://bugs.python.org/issue30473 in py36
        start_times = defaultdict(lambda: datetime.datetime.now())
        max_stage_id = -1

        status = self.sc.statusTracker()
        while True:
            with self.condition:
                if self.sc._jsc is None or not self.alive:
                    # End the thread
                    self.paused = True
                    break
                elif self.paused:
                    # Pause the thread
                    self.condition.wait()
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
                    s = _format_stage_info(self.bar_width, stage_info, td, self.timedelta_formatter)
                    progressbar_list.append(s)
                if stage_counter == 3:
                    break

            # Ensure that when we get a new maximum stage id we print a \n
            # to make the progress bar go on to the next line.
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
            time.sleep(self.sleep_time)


_printer_singleton = None


def start(sc, timedelta_formatter=_pretty_time_delta, bar_width=20, sleep_time=0.5):
    """Creates a :class:`ProgressPrinter` that polls the SparkContext for information
    about active stage progress and prints that information to stderr.

    The printer runs in a thread and is useful for showing text-based
    progress bars in interactive environments (e.g., REPLs, Jupyter Notebooks).

    This function creates a singleton printer instance and returns that instance
    no matter what arguments are passed to this function again until :func:`stop`
    is called to shutdown the singleton. If you want more control over the printer
    lifecycle, create an instance of :class:`ProgressPrinter` directly and use its
    methods.

    Parameters
    ----------
    sc: :class:`pyspark.context.SparkContext`, optional
        SparkContext to use to create a new thread
    timedelta_formatter : callable, optional
        Converts a timedelta to a string.
    bar_width : int, optional
        Width of the progressbar to print out.
    sleep_time : float, optional
        Frequency in seconds with which to poll Apache Spark for task stage information.

    Returns
    -------
    :class:`ProgressPrinter`
    """
    global _printer_singleton

    if _printer_singleton is None:
        _printer_singleton = ProgressPrinter(sc, timedelta_formatter, bar_width, sleep_time)
        _printer_singleton.start()
    return _printer_singleton


def stop():
    """Shuts down the singleton :class:`ProgressPrinter` instance created by :func:`start`.

    Does nothing if start was never called.
    """
    global _printer_singleton

    if _printer_singleton is not None:
        _printer_singleton.shutdown()
        _printer_singleton = None


# Deprecated: Retain old name for backwards compatibility for the time being
start_spark_progress_bar_thread = start
