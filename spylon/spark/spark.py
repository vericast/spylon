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


"""
Utility classes for creating and interacting with the java side of pyspark.

"""
from __future__ import absolute_import, print_function
from spylon.common import JVMHelpers

# noinspection PyUnresolvedReferences
from pyspark import SparkContext
# noinspection PyUnresolvedReferences
from pyspark.sql import Column, SQLContext
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col as _make_col
# noinspection PyUnresolvedReferences
from pyspark.sql.column import _to_seq, _to_list, _to_java_column

class SparkJVMHelpers(JVMHelpers):
    """Utilities for easier interoperability with pyspark and scala.

    Parameters
    ----------
    sc : SparkContext
        A pyspark context that you'd like to use as the main jvm reflection point.
    """

    def __init__(self, sc):
        super(SparkJVMHelpers, self).__init__()
        self.sc = sc

    @property
    def jvm(self):
        """Returns the py4j jvm mirror that spark is using.
        """
        return self.sc._jvm

    @property
    def gateway(self):
        """Returns the py4j gateway that spark is using.
        """
        return self.sc._gateway

    @property
    def classloader(self):
        """Returns the private class loader that spark uses.

        This is needed since jars added with --jars are not easily resolvable by py4j's classloader
        """
        return self.jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()

    def get_java_container(self, package_name=None, object_name=None, java_class_instance=None):
        """Convenience method to get the container that houses methods we wish to call a method on.

        """
        if package_name is not None:
            jcontainer = self.import_scala_package_object(package_name)
        elif object_name is not None:
            jcontainer = self.import_scala_object(object_name)
        elif java_class_instance is not None:
            jcontainer = java_class_instance
        else:
            raise RuntimeError("Expected one of package_name, object_name or java_class_instance")
        return jcontainer

    def wrap_function_cols(self, name, package_name=None, object_name=None, java_class_instance=None, doc=""):
        """Utility method for wrapping a scala/java function that returns a spark sql Column.

        This assumes that the function that you are wrapping takes a list of spark sql Column objects as its arguments.
        """
        def _(*cols):
            jcontainer = self.get_java_container(package_name=package_name, object_name=object_name, java_class_instance=java_class_instance)
            # Ensure that your argument is a column
            col_args = [col._jc if isinstance(col, Column) else _make_col(col)._jc for col in cols]
            function = getattr(jcontainer, name)
            args = col_args
            jc = function(*args)
            return Column(jc)
        _.__name__ = name
        _.__doc__ = doc
        return _

    def wrap_spark_sql_udf(self, name, package_name=None, object_name=None, java_class_instance=None, doc=""):
        """Wraps a scala/java spark user defined function """
        def _(*cols):
            jcontainer = self.get_java_container(package_name=package_name, object_name=object_name, java_class_instance=java_class_instance)
            # Ensure that your argument is a column
            function = getattr(jcontainer, name)
            judf = function()
            jc = judf.apply(self.to_scala_seq([_to_java_column(c) for c in cols]))
            return Column(jc)
        _.__name__ = name
        _.__doc__ = doc
        return _

    def wrap_java_dataframe(self, jdf):
        """Converts a java scala spark dataframe to its python equivalent."""
        return pyspark.sql.Dataframe(jdf, self.sc)

    # TODO: Future work could be made that will make use of the catalyst code generator to directly create classes from strings of Java code
    # def compile_body(self):
    #     self.import_scala_object("org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator")
    #     self.c CodeAndComment
