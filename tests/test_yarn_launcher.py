from __future__ import print_function, absolute_import
from spylon.spark import prepare_pyspark_yarn_interactive
import spylon.spark.launcher as sparklauncher
import os

from spylon.spark.yarn_launcher import _extract_local_archive, create_conda_env, archive_dir


def test_prepare_interactive():
    c = sparklauncher.SparkConfiguration()

    new_conf = prepare_pyspark_yarn_interactive("conda", "hdfs://some/env/conda.zip", c)

    # Must be a new instance not a copy.
    assert new_conf is not c

    expected_python = os.path.join(".", "CONDA", "conda", "bin", "python")
    assert new_conf._python_path == expected_python
    # archive must be added tp the arguments that will be supplied.
    assert "hdfs://some/env/conda.zip#CONDA" in new_conf.archives
    assert os.environ["PYSPARK_PYTHON"] == expected_python


def test_cleanup(tmpdir_factory):
    #
    cwd = str(tmpdir_factory.mktemp("test_spylon"))

    # Create a simple conda environment
    env_dir, env_name = create_conda_env(cwd, 'test_spylon', ['python=3.5'])
    # zip it up
    env_archive = archive_dir(env_dir)

    # Extract the dir to a known location.
    extract_dir = os.path.join(cwd, "extract")
    os.mkdir(extract_dir)
    cleanup_functions = []
    _extract_local_archive(extract_dir, cleanup_functions, env_name=env_name, local_archive=env_archive)

    # Run the cleanup functions
    for fn in cleanup_functions:
        fn()

    # We should have cleaned up everything
    assert len(os.listdir(extract_dir)) == 0
