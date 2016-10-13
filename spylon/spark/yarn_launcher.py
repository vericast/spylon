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


from __future__ import print_function, absolute_import

import copy
import subprocess
import os
import sys
import yaml
import shlex
import zipfile
import logging
import pprint
import platform

__author__ = 'mniekerk'

log = logging.getLogger("spylon.spark.yarn_launcher")


def create_conda_env(sandbox_dir, env_name, dependencies, options=()):
    """
    Create a conda environment inside the current sandbox for the given list of dependencies and options.

    Parameters
    ----------
    sandbox_dir : str
    env_name : str
    dependencies : list
        List of conda specs
    options
        List of additional options to pass to conda.  Things like ["-c", "conda-forge"]

    Returns
    -------
    (env_dir, env_name)
    """

    env_dir = os.path.join(sandbox_dir, env_name)
    cmdline = ["conda", "create", "--yes", "--copy", "--quiet", "-p", env_dir] + list(options) + dependencies

    log.info("Creating conda environment: ")
    log.info("  command line: %s", cmdline)
    subprocess.check_call(cmdline, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    log.debug("Environment created")

    return env_dir, env_name


def archive_dir(env_dir):
    """
    Compresses the directory and writes to its parent

    Parameters
    ----------
    env_dir : str

    Returns
    -------
    str
    """
    output_filename = env_dir + ".zip"
    log.info("Archiving conda environment: %s -> %s", env_dir, output_filename)
    subprocess.check_call(["zip", "-r", "-0", "-q", output_filename, env_dir])
    return output_filename


# TODO : move this into the SparkConfiguration object
def prepare_pyspark_yarn_interactive(env_name, env_archive, spark_conf):
    """
    This ASSUMES that you have a compatible python environment running on the other side.

    WARNING: Injects "PYSPARK_DRIVER_PYTHON" and "PYSPARK_PYTHON" as
    environmental variables into your current environment

    Parameters
    ----------
    env_name : str
    env_archive : str
    spark_conf : SparkConfiguration

    Examples
    --------
    >>> from spylon.spark import SparkConfiguration
    >>> conf = SparkConfiguration()
    >>> import spylon.spark.yarn_launcher as yl
    >>> conf = yl.prepare_pyspark_yarn_interactive(
    ...    env_name="yarn-pyspark-env", env_archive="hdfs:///path/to/conda_envs/yarn-pyspark-env.zip",
    ...    spark_conf=conf
    ... )
    ... # Create our context
    ... sc, sqlC = conf.sql_context("conda-test")
    ... # Example of it working
    ... rdd = sc.parallelize(range(10), 10)
    ...
    ... def pandas_test(x):
    ...    import numpy
    ...    import pandas
    ...    import sys
    ...    import socket
    ...    return [{"numpy": numpy.__version__, "pandas": pandas.__version__,
    ...             "host": socket.getfqdn(), "python": sys.executable}]
    ...
    ... rdd.mapPartitions(pandas_test).collect()

    Returns
    -------
    SparkConfiguration
        Copy of `spark_conf` input with added Yarn requirements.
    """

    from .launcher import SparkConfiguration

    assert isinstance(spark_conf, SparkConfiguration)

    yarn_python = os.path.join(".", "CONDA", env_name, "bin", "python")
    archives = env_archive + "#CONDA"

    new_spark_conf = copy.deepcopy(spark_conf)
    new_spark_conf.master = "yarn"
    new_spark_conf.deploy_mode = "client"
    new_spark_conf.archives = [archives]
    new_spark_conf.conf.set("spark.executorEnv.PYSPARK_PYTHON", yarn_python)
    new_spark_conf._python_path = yarn_python

    env_update = {
        "PYSPARK_DRIVER_PYTHON": sys.executable,
        "PYSPARK_PYTHON": yarn_python
    }

    os.environ.update(env_update)

    return new_spark_conf


def run_pyspark_yarn_client(env_dir, env_name, env_archive, args):
    """
    Initializes the requires spark command line options on order to start a python job with the given python
    environment.

    Parameters
    ----------
    env_dir : str
    env_name : str
    env_archive : str
    args : list

    Returns
    -------
    This call will spawn a child process and block until that is complete.
    """
    env = dict(os.environ)
    python = os.path.join(env_dir, "bin", "python")
    yarn_python = os.path.join(".", "CONDA", env_name, "bin", "python")
    archives = env_archive + "#CONDA"

    prepend_args = [
        "--master", "yarn",
        "--deploy-mode", "client",
        "--archives", archives,
    ]

    env_update = {
        "PYSPARK_DRIVER_PYTHON": python,
        "PYSPARK_PYTHON": yarn_python
    }

    env.update(env_update)
    spark_submit = os.path.join(env["SPARK_HOME"], "bin", "spark-submit")

    log.info("Running spark in YARN-client mode with added arguments")
    log.info("  args: %s", pprint.pprint(prepend_args, indent=4))
    log.info("  env: %s", pprint.pprint(env_update, indent=4))

    # REPLACE our python process with another one
    subprocess.check_call([spark_submit] + prepend_args + args, env=env)


def run_pyspark_yarn_cluster(env_dir, env_name, env_archive, args):
    """
    Initializes the requires spark command line options on order to start a python job with the given python environment.

    Parameters
    ----------
    env_dir : str
    env_name : str
    env_archive : str
    args : list

    Returns
    -------
    This call will spawn a child process and block until that is complete.
    """
    env = dict(os.environ)
    yarn_python = os.path.join(".", "CONDA", env_name, "bin", "python")
    archives = env_archive + "#CONDA"

    prepend_args = [
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON={}".format(yarn_python),
        "--archives", archives,
    ]

    env_update = {
        "PYSPARK_PYTHON": yarn_python
    }

    env.update(env_update)
    spark_submit = os.path.join(env["SPARK_HOME"], "bin", "spark-submit")

    log.info("Running spark in YARN-client mode with added arguments")
    log.info("  args: %s", pprint.pprint(prepend_args, indent=4))
    log.info("  env: %s", pprint.pprint(env_update, indent=4))
    # REPLACE our python process with another one
    subprocess.check_call([spark_submit] + prepend_args + args, env=env)


def launcher(deploy_mode, args, working_dir="."):
    """Initializes arguments and starts up pyspark with the correct deploy mode and environment.

    Parameters
    ----------
    deploy_mode : {"client", "cluster"}
    args : str
        arguments to pass onwards to spark submit
    working_dir : str
        path to working directory to use for creating conda environments

    Returns
    -------
    This call will spawn a child process and block until that is complete.
    """
    # Splits the list of arguments to Spark arguments and non-spark arguments.
    spark_args = args

    # Scan through the arguments to find --conda
    # TODO: make this optional,  if not specified ignore all the python stuff
    i = spark_args.index("--conda-env")
    conda_env = spark_args[i+1]
    spark_args = spark_args[:i] + spark_args[i+2:]

    assert isinstance(conda_env, str)
    # "hadoop fs -ls" can return URLs with only a single "/" after the "hdfs:" scheme
    if conda_env.startswith("hdfs:/"):
        log.info("Using conda environment from hdfs location")
        # conda environment is on hdfs
        filename = os.path.basename(conda_env)
        env_name, _ = os.path.splitext(filename)

        # When running in client mode download it from HDFS first.
        if deploy_mode == "client":
            # TODO: Allow user to specify a local environment to use if around

            subprocess.check_call(["hadoop", "fs", "-get", conda_env, working_dir], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            with zipfile.ZipFile(os.path.join(working_dir, filename)) as z:
                z.extractall(working_dir)
            env_dir = os.path.join(working_dir, env_name)
            # Because of a python deficiency (Issue15795), the execute bits aren't
            # preserved when the zip file is unzipped. Need to add them back here.
            _fix_permissions(env_dir)
        else:
            env_dir = ""
        env_archive = conda_env

    # We have a precreated conda environment around.
    elif conda_env.endswith(".zip"):
        log.info("Using conda environment from zip file")
        with zipfile.ZipFile(conda_env) as z:
            basename, _ = os.path.splitext(conda_env)
            z.extractall(working_dir)

        env_name = os.path.basename(basename)
        env_dir = os.path.join(working_dir, env_name)
        env_archive = os.path.abspath(conda_env)
        # Because of a python deficiency (Issue15795), the execute bits aren't
        # preserved when the zip file is unzipped. Need to add them back here.
        _fix_permissions(env_dir)

    # The case where we have to CREATE the environment ourselves
    elif conda_env.endswith(".yaml"):
        log.info("Building conda environment from yaml specification")
        with open(conda_env) as fo:
            env = yaml.load(fo)

        conda_create_args = env.get("conda-args", "")
        conda_create_args = shlex.split(conda_create_args)
        deps = env["dependencies"]
        env_name = env.get("name", "envname")

        # Create the conda environment
        env_dir, env_name = create_conda_env(working_dir, env_name, deps, conda_create_args)

        # Archive the conda environment
        env_archive = archive_dir(env_dir)

    else:
        raise NotImplementedError()

    args = dict(env_dir=env_dir, env_name=env_name, env_archive=env_archive, args=spark_args)
    if deploy_mode == "client":
        run_pyspark_yarn_client(**args)
    elif deploy_mode == "cluster":
        run_pyspark_yarn_cluster(**args)


def _fix_permissions(env_dir):
    bin_dir = os.path.join(env_dir, "bin")
    if os.path.isdir(bin_dir):
        if platform.system() == 'Linux':
            subprocess.check_call(["chmod", "-R", "a+x", bin_dir])
        else:
            raise NotImplementedError("Don't know how to change permissions on " + platform.system())


def pyspark_conda_yarn_cluster():
    """
    Endpoint for starting a pyspark conda job using cluster-mode.

    """
    args = sys.argv[1:]
    launcher("cluster", args)


def pyspark_conda_yarn_client():
    """
    Endpoint for starting a pyspark conda job using client-mode.

    """
    args = sys.argv[1:]
    launcher("client", args)
