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

""""
A better launcher from the python side for creating a pyspark context

This allows you to much more easily customize the parameters that get passed to
spark-submit that ultimately gets launched when instantiating a SparkContext.

This does NOT require you to mangle sys.path yourself in order to be able to import
pyspark.

This is done via minRK's findspark library if you cannot import pyspark directly
"""
from __future__ import absolute_import, print_function

import logging
from collections import defaultdict
import os
import sys
from contextlib import contextmanager
import pandas as pd
from six import iteritems
from spylon.common import _as_iterable

log = logging.getLogger("spylon.spark.launcher")


try:
    import pyspark
    use_findspark = False
except ImportError:
    log.debug("pyspark not importable.  using findspark instead")
    use_findspark = True

if use_findspark:
    import findspark

    def init_spark(spark_home=None):
        if spark_home is None:
            spark_home = os.environ["SPARK_HOME"]
        findspark.init_spark(spark_home=spark_home)
        import pyspark
else:
    def init_spark(spark_home=None):
        # Assumes all things are good.
        if spark_home is None:
            spark_home = os.environ["SPARK_HOME"]
        os.environ["SPARK_HOME"] = spark_home

DEFAULT_SPARK_PACKAGES = ()
DEFAULT_PYTHON = 'python'
_SPARK_INITIALIZED = False


def keyfilter(predicate, d, factory=dict):
    """ Filter items in dictionary by key

    >>> iseven = lambda x: x % 2 == 0
    >>> d = {1: 2, 2: 3, 3: 4, 4: 5}
    >>> keyfilter(iseven, d)
    {2: 3, 4: 5}

    See Also:
        valfilter
        itemfilter
        keymap
    """
    rv = factory()
    for k, v in iteritems(d):
        if predicate(k):
            rv[k] = v
    return rv


class _AttributedDict(object):
    """Utility attributed dictionary with Mappable semantics.

    This has the option to delegate setting to another Mappable if needed.
    This effectively allows us to do something like

    x.foo.bar.baz = 7

    and have that effectively turn into

    y["foo.bar.baz"] = 7

    """

    def __init__(self, init=None, parent=None, surrogate=None, attr=None):
        self._enable_surrogate = False
        self._attr = attr
        self._parent = parent
        self._surrogate = surrogate
        if init is None:
            init = {}
        for k, v in init.items():
            self[k] = v
        self._enable_surrogate = True

    def __getstate__(self):
        return self.__dict__.items()

    def __setstate__(self, items):
        for key, val in items:
            self.__dict__[key] = val

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.__dict__.__repr__())

    def _repr_pretty_(self, p, cycle):
        from IPython.lib.pretty import _dict_pprinter_factory
        fn = _dict_pprinter_factory('{', '}')
        excluded_fields = {'_enable_surrogate', '_attr', '_parent', '_surrogate'}
        fn(keyfilter(lambda k: k not in excluded_fields, self.__dict__), p, cycle)

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setattr__(self, key, value):
        if key.startswith('_') or (not self._enable_surrogate):
            super(_AttributedDict, self).__setattr__(key, value)
        else:
            full_key = [key]
            parent = self
            while parent:
                attr = parent._attr
                if attr is None:
                    break
                full_key.append(attr)
                parent = parent._parent

            dotted_key = '.'.join(reversed(full_key))
            self._surrogate[dotted_key] = value

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = self.__class__(value, self, self._surrogate, key)

        old, self._enable_surrogate = (self._enable_surrogate, False)
        setattr(self, key, value)
        self._enable_surrogate = old

    def __dir__(self):
        excluded = ['_attr', '_enable_surrogate', '_parent', '_surrogate']
        x = self.__dict__.keys()
        return [e for e in x if e not in excluded]


class _SparkProperty(object):
    """Utility class for representing a spark property with its documentation.

    """

    def __init__(self, propname, default, meaning, active_value_dict):
        self.property_name = propname
        self.default = default
        self.meaning = meaning
        self.active_value_dict = active_value_dict

    def __dir__(self):
        return []

    def __str__(self):
        return self.property_name

    def _current_value(self):
        propval = self.active_value_dict.get(self.property_name)
        if propval is not None:
            return '<SET>: {}'.format(propval)
        else:
            return '<UNSET>'

    def __repr__(self):
        return "SparkProperty {3} ({0!r}, default={1!r}, meaning={2!r})".format(
            self.property_name,
            self.default,
            self.meaning,
            self._current_value()
        )

    def _repr_html_(self):
        import cgi
        return """
        <table><tr>
            <td>{0}</td>
            <td>{3}</td>
            <td>{1}</td>
            <td>{2}</td>
        </tr></table>
        """.format(cgi.escape(self.property_name), cgi.escape(self.default), cgi.escape(self.meaning),
                   cgi.escape(self._current_value()))


def _tree():
    return defaultdict(_tree)


class _SparkConfHelper(object):

    def __init__(self, version='latest', existing_conf=None):
        self.all_properties = {}

        if existing_conf is not None:
            assert isinstance(existing_conf, dict)
            conf = existing_conf.copy()
        else:
            conf = {}

        # Initializes a list of known properties for spark.  This is pretty high on magic
        allProps = pd.concat(list(self._load_documentation(version=version)))
        self._conf_dict = conf

        d = _tree()
        for pn, default, meaning in allProps.itertuples(index=False):
            prop = _SparkProperty(pn, default, meaning, self._conf_dict)
            self.all_properties[pn] = prop

            parts = pn.split('.')
            node = d
            for part in parts[0:-1]:
                node = node[part]

            if isinstance(node, dict):
                node[parts[-1]] = prop
            else:
                continue
        self.spark = _AttributedDict(d, surrogate=self._conf_dict).spark

    def _load_documentation(self, version):
        doc_urls = [
            # Bulk of spark properties
            "https://spark.apache.org/docs/{version}/configuration.html",
            # Spark SQL properties appear only in here for some reaspn
            "https://spark.apache.org/docs/{version}/sql-programming-guide.html",
        ]

        for url in doc_urls:
            doc_url = url.format(version=version)
            log.debug("Loading spark properties from %s", doc_url)
            dfs = pd.read_html(doc_url, header=0)
            for df in dfs:
                if ("Property Name" in df) and ('Default' in df):
                    yield df[["Property Name", "Default", "Meaning"]]

    def __repr__(self):
        return "ConfiguredProperties %s" % (self._conf_dict.__repr__())

    def _repr_pretty_(self, p, cycle):
        from IPython.lib.pretty import _dict_pprinter_factory
        fn = _dict_pprinter_factory('ConfiguredProperties {', '}')
        fn(self._conf_dict, p, cycle)

    def __setitem__(self, key, value):
        self._conf_dict[key] = value

    def set(self, key, value):
        self._conf_dict[key] = value
        return self


class SparkConfiguration(object):
    _boolean_args = {'verbose'}
    _spark_launcher_arg_names = {
        'master', 'deploy-mode', 'jars', 'packages', 'exclude-packages', 'repositories', 'py-files', 'files',
        'properties-file', 'driver-memory', 'driver-java-options', 'driver-library-path', 'driver-class-path',
        'driver-cores', 'executor-memory', 'proxy-user', 'verbose', 'executor-cores'
    }
    _spark_launcher_arg_sep = {'driver-java-options': ' ',
                               'driver-library-path': ':',
                               'driver-class-path': ':'}

    _default_spark_conf = {}
    _default_spark_launcher_args = {}

    def __repr__(self):
        import textwrap
        return textwrap.dedent("""
        SparkConfiguration:
            launcher_arguments: {0}
            conf: {1!r}
        """.format(self._spark_launcher_args, self._spark_conf_helper)
                               )

    def _repr_pretty_(self, p, cycle):
        """Pretty printer for the spark cnofiguration"""
        from IPython.lib.pretty import RepresentationPrinter
        assert isinstance(p, RepresentationPrinter)

        p.begin_group(1, "SparkConfiguration(")

        def kv(k, v, do_comma=True):
            p.text(k)
            p.pretty(v)
            if do_comma:
                p.text(", ")
            p.breakable()

        kv("launcher_arguments: ", self._spark_launcher_args)
        kv("conf: ", self._spark_conf_helper)
        kv("spark_home: ", self.spark_home)
        kv("python_path: ", self._python_path, False)

        p.end_group(1, ')')

    def __init__(self, python_path=None, spark_conf=None, spark_launcher_args=None):
        self._spark_launcher_args = spark_launcher_args or self._default_spark_launcher_args
        self._python_path = python_path or "python"
        self._spark_home = None
        self._spark_conf_helper = _SparkConfHelper(existing_conf=spark_conf or self._default_spark_conf)

    def __dir__(self):
        """Since we have some parameters that are special we want to allow pulling them out for directory listing"""
        return sorted(set(
            dir(type(self))
            + list(self.__dict__)
            + list(_.replace('-', '_') for _ in self._spark_launcher_arg_names)
        ))

    def __setattr__(self, key, value):
        """SetAttr for setting spark-submit launcher arguments"""
        assert (isinstance(key, str))
        spark_arg = key.replace('_', '-')
        if key.startswith("_"):
            return super(SparkConfiguration, self).__setattr__(key, value)

        if spark_arg in self._spark_launcher_arg_names:
            self._spark_launcher_args[spark_arg] = value

    def __getattr__(self, key):
        assert (isinstance(key, str))
        if key.startswith("_"):
            return super(SparkConfiguration, self).__getattribute__(key)
        spark_arg = key.replace('_', '-')
        if spark_arg in self._spark_launcher_arg_names:
            return self._spark_launcher_args[spark_arg]

    def __setitem__(self, key, val):
        return self._spark_conf.__setitem__(key, val)

    def __getitem__(self, key):
        return self._spark_conf.__getitem__(key)

    def _set_environment_variables(self):
        """Initializes the correct environment variables for spark"""
        cmd = []
        for key, val in self._spark_launcher_args.items():
            if val is None:
                continue
            val = list(_as_iterable(val))
            if len(val):
                if key in self._boolean_args:
                    cmd.append("--{key}".format(key=key))
                else:
                    sep = self._spark_launcher_arg_sep.get(key, ',')
                    cmd.append('--{key} {val}'.format(key=key, val=sep.join(str(x) for x in val)))

        cmd += ['pyspark-shell']
        cmd_line = ' '.join(x for x in cmd if x)
        os.environ["PYSPARK_SUBMIT_ARGS"] = cmd_line
        log.info("spark-submit arguments: %s", cmd_line)

    def _init_spark(self):
        """Initializes spark so that pyspark is importable.  This also sets up the required environment variables
        """
        global _SPARK_INITIALIZED
        spark_home = self.spark_home
        python_path = self._python_path

        if _SPARK_INITIALIZED:
            if spark_home == os.environ["SPARK_HOME"]:
                # matches with already initialized
                pass
            else:
                # findspark adds two path to the search path.
                sys.path.pop(0)
                sys.path.pop(0)
                findspark.init(spark_home=spark_home, edit_rc=False, edit_profile=False, python_path=python_path)
        else:
            findspark.init(spark_home=spark_home, edit_rc=False, edit_profile=False, python_path=python_path)

        _SPARK_INITIALIZED = True
        self._set_environment_variables()

    @property
    def conf(self):
        return self._spark_conf_helper

    @property
    def launcher_args(self):
        return self._spark_launcher_args

    @property
    def spark_home(self):
        return self._spark_home

    def spark_context(self, application_name):
        """Create a spark context given the parameters configured in this class.

        The caller is responsible for calling ``.close`` on the resulting spark context

        Parameters
        ----------
        application_name : string

        Returns
        -------
        sc : SparkContext
        """

        # initialize the spark configuration
        self._init_spark()
        import pyspark
        import pyspark.sql

        # initialize conf
        spark_conf = pyspark.SparkConf()
        for k, v in self._spark_conf_helper._conf_dict.items():
            spark_conf.set(k, v)

        log.info("Starting SparkContext")
        return pyspark.SparkContext(appName=application_name, conf=spark_conf)

    def sql_context(self, application_name):
        """Create a spark context given the parameters configured in this class.

        The caller is responsible for calling ``.close`` on the resulting spark context

        Parameters
        ----------
        application_name : string

        Returns
        -------
        sc : SparkContext
        """
        sc = self.spark_context(application_name)
        import pyspark
        sqlContext = pyspark.SQLContext(sc)
        return (sc, sqlContext)


default_configuration = SparkConfiguration()


@contextmanager
def with_spark_context(application_name, conf=None):
    """Context manager for a spark context

    Parameters
    ----------
    application_name : string
    conf : string, optional

    Returns
    -------
    sc : SparkContext

    Examples
    -------
    Used within a context manager
    >>> with with_spark_context("MyApplication") as sc:
    ...     # Your Code here
    ...

    """
    if conf is None:
        conf = default_configuration
    assert isinstance(conf, SparkConfiguration)

    sc = conf.spark_context(application_name)
    try:
        yield sc
    finally:
        sc.stop()


@contextmanager
def with_sql_context(application_name, conf=None):
    """Context manager for a spark context

    Returns
    -------
    sc : SparkContext
    sql_context: SQLContext

    Examples
    -------
    Used within a context manager
    >>> with with_sql_context("MyApplication") as (sc, sql_context):
    ...     import pyspark
    ...     # Do stuff
    ...     ...

    """
    if conf is None:
        conf = default_configuration
    assert isinstance(conf, SparkConfiguration)

    sc = conf.spark_context(application_name)
    import pyspark.sql
    try:
        yield sc, pyspark.sql.SQLContext(sc)
    finally:
        sc.stop()
