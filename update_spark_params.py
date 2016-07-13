"""
Utility used to regenerate the json files used for the lists of spark properties
"""

spark_versions = ["1.6.0", "1.6.1", "latest"]
from spylon.spark.launcher import _save_documentation

for sv in spark_versions:
    _save_documentation(version=sv)
