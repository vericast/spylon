Changelog
=========

0.2.12
------

 - [PR-43](https://github.com/maxpoint/spylon/pull/43) Fix broken getattr
   introduced in [PR-39](https://github.com/maxpoint/spylon/pull/39) and add
   better testing around the SparkConfiguration getattr/setattr
 - [PR-40](https://github.com/maxpoint/spylon/pull/40) Update json spark
   properties for 1.6.0, 1.6.1, 2.0.2, 2.1.0 and latest. Pull code out of
   spark/launcher.py and shove it into update_spark_params.py

0.2.11
------
 - [PR-39](https://github.com/maxpoint/spylon/pull/39) Fix getattr bug

0.2.10
------

 - Daemonize the progress thread to avoid hanging the process [PR-33](https://github.com/maxpoint/spylon/pull/33)

0.2.9
-----

 - Improve pip installation experience [PR-29](https://github.com/maxpoint/spylon/pull/29)

0.2.8
-----
 - Bugfix release.

0.2.7
-----
 - Cleanup of downloaded / extracted artifacts when using spylon.spark.yarn_launcher.

0.2.5
-----
 - Bugfix to ensure that various driver side settings will be correctly set for the spark-submit launcher arguments
   if set in spark_conf.

