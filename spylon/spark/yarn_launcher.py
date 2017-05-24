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
import shutil
import posixpath

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


def _conda_from_hdfs(conda_env, deploy_mode, working_dir, cleanup_functions):
    log.info("Using conda environment from hdfs location")
    # conda environment is on hdfs
    filename = os.path.basename(conda_env)
    env_name, _ = os.path.splitext(filename)

    # When running in client mode download it from HDFS first.
    if deploy_mode == "client":
        # TODO: Allow user to specify a local environment to use if around

        subprocess.check_call(["hadoop", "fs", "-get", conda_env, working_dir], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        local_archive = os.path.join(working_dir, filename)
        env_dir = _extract_local_archive(working_dir, cleanup_functions, env_name, local_archive)
    else:
        env_dir = ""
    env_archive = conda_env

    return env_name, env_dir, env_archive


def _conda_from_zip(conda_env, deploy_mode, working_dir, cleanup_functions):
    log.info("Using conda environment from zip file")
    env_archive = conda_env
    basename, _ = os.path.splitext(env_archive)
    env_name = os.path.basename(basename)
    env_dir = _extract_local_archive(working_dir, cleanup_functions, env_name, env_archive)
    return env_name, env_dir, env_archive


def _conda_from_yaml(conda_env, deploy_mode, working_dir, cleanup_functions):
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

    abs_env_dir = os.path.abspath(env_dir)
    abs_env_archive = os.path.abspath(env_archive)

    cleanup_functions.extend([
        lambda: shutil.rmtree(abs_env_dir),
        lambda: os.unlink(abs_env_archive),
    ])
    return env_name, env_dir, env_archive


def launcher(deploy_mode, args, working_dir=".", cleanup=True):
    """Initializes arguments and starts up pyspark with the correct deploy mode and environment.

    Parameters
    ----------
    deploy_mode : {"client", "cluster"}
    args : list
        Arguments to pass onwards to spark submit.
    working_dir : str, optional
        Path to working directory to use for creating conda environments.  Defaults to the current working directory.
    cleanup : bool, optional
        Clean up extracted / generated files.  This defaults to true since conda environments can be rather large.

    Returns
    -------
    This call will spawn a child process and block until that is complete.
    """
    spark_args = args.copy()
    # Scan through the arguments to find --conda
    # TODO: make this optional,  if not specified ignore all the python stuff
    # Is this double dash in front of conda env correct?
    i = spark_args.index("--conda-env")
    # pop off the '--conda-env' portion and just drop it on the floor
    spark_args.pop(i)
    # Now pop off the actual conda env var passed to the launcher
    conda_env = spark_args.pop(i)
    cleanup_functions = []
    # What else could this possibly be other than a string here?
    assert isinstance(conda_env, str)
    func_kwargs = {'conda_env': conda_env,
                   'deploy_mode': deploy_mode,
                   'working_dir': working_dir,
                   'cleanup_functions': cleanup_functions}
    if conda_env.startswith("hdfs:/"):
        # "hadoop fs -ls" can return URLs with only a single "/" after the "hdfs:" scheme
        env_name, env_dir, env_archive = _conda_from_hdfs(**func_kwargs)
    elif conda_env.endswith(".zip"):
        # We have a precreated conda environment around.
        env_name, env_dir, conda_env = _conda_from_zip(**func_kwargs)
    elif conda_env.endswith(".yaml"):
        # The case where we have to CREATE the environment ourselves
        env_name, env_dir, env_archive = _conda_from_yaml(**func_kwargs)
    else:
        raise NotImplementedError("Can only run launcher if your conda env is on hdfs (starts "
                                  "with 'hdfs:/', is already a zip (ends with '.zip'), or is "
                                  "coming from a yaml specification (ends with '.yaml' and "
                                  "conforms to the conda environment.yaml spec)")
    del func_kwargs

    func_kwargs = dict(env_dir=env_dir, env_name=env_name, env_archive=env_archive, args=spark_args)
    funcs = {'client': run_pyspark_yarn_client, 'cluster': run_pyspark_yarn_cluster}
    try:
        funcs[deploy_mode](**func_kwargs)
    finally:
        if not cleanup:
            return
        # iterate over and call all cleanup functions
        for function in cleanup_functions:
            try:
                function()
            except:
                log.exception("Cleanup function %s failed", function)


def _extract_local_archive(working_dir, cleanup_functions, env_name, local_archive):
    """Helper internal function for extracting a zipfile and ensure that a cleanup is queued.

    Parameters
    ----------
    working_dir : str
    cleanup_functions : List[() -> NoneType]
    env_name : str
    local_archive : str
    """
    with zipfile.ZipFile(local_archive) as z:
        z.extractall(working_dir)
        archive_filenames = z.namelist()

    root_elements = {m.split(posixpath.sep, 1)[0] for m in archive_filenames}
    abs_archive_filenames = [os.path.abspath(os.path.join(working_dir, f)) for f in root_elements]

    def cleanup():
        for fn in abs_archive_filenames:
            if os.path.isdir(fn):
                shutil.rmtree(fn)
            else:
                os.unlink(fn)

    cleanup_functions.append(cleanup)
    env_dir = os.path.join(working_dir, env_name)
    # Because of a python deficiency (Issue15795), the execute bits aren't
    # preserved when the zip file is unzipped. Need to add them back here.
    _fix_permissions(env_dir)
    return env_dir


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
