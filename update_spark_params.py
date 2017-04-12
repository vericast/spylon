"""
Utility used to regenerate the json files used for the lists of spark properties
"""
import json
import numpy
import pandas as pd
from os.path import join, dirname
import pdb
import sys
# Next three imports are indirectly required by the code in this function
import html5lib
import bs4
import lxml


def _save_documentation(version, base_url="https://spark.apache.org/docs"):
    """
    Write the spark property documentation to a file
    """
    target_dir = join(dirname(__file__), 'spylon', 'spark')
    with open(join(target_dir, "spark_properties_{}.json".format(version)), 'w') as fp:
        all_props = _fetch_documentation(version=version, base_url=base_url)
        all_props = sorted(all_props, key=lambda x: x[0])
        all_props_d = [{"property": p, "default": d, "description": desc} for p, d, desc in all_props]
        json.dump(all_props_d, fp, indent=2)


def _fetch_documentation(version, base_url="https://spark.apache.org/docs"):
    doc_urls = [
        "{base_url}/{version}/configuration.html",
        "{base_url}/{version}/sql-programming-guide.html",
        "{base_url}/{version}/monitoring.html",
        "{base_url}/{version}/spark-standalone.html",
        "{base_url}/{version}/running-on-mesos.html",
        "{base_url}/{version}/running-on-yarn.html",
    ]

    for url in doc_urls:
        doc_url = url.format(version=version, base_url=base_url)
        # print(url)
        print("Loading spark properties from %s", doc_url)
        dfs = pd.read_html(doc_url, header=0)
        desired_cols = ["Property Name", "Default", "Meaning"]
        for df in dfs:
            if ("Property Name" in df) and ('Default' in df):
                for pn, default, desc in df[desired_cols].itertuples(index=False):
                    if type(default) == numpy.bool_:
                        default = bool(default)
                    yield pn, default, desc


if __name__ == "__main__":
    # set the pdb_hook as the except hook for all exceptions so that debugging is easier
    def pdb_hook(exctype, value, traceback):
        pdb.post_mortem(traceback)


    sys.excepthook = pdb_hook
    spark_versions = ["1.6.0", "1.6.1", "2.0.2", "2.1.0", "latest"]
    for sv in spark_versions:
        _save_documentation(version=sv)


