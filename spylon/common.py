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
from abc import ABCMeta, abstractmethod, abstractproperty
from six import add_metaclass, string_types


def as_iterable(iterable_or_scalar):
    """Utility for converting an object to an iterable.

    Parameters
    ----------
    iterable_or_scalar : anything

    Returns
    -------
    l : iterable
        If `obj` was None, return the empty tuple.
        If `obj` was not iterable returns a 1-tuple containing `obj`.
        Otherwise return `obj`

    Notes
    -----
    Although both string types and dictionaries are iterable in Python, we are treating them as not iterable in this
    method.  Thus, as_iterable(dict()) returns (dict, ) and as_iterable(string) returns (string, )

    Exammples
    ---------
    >>> as_iterable(1)
    (1,)
    >>> as_iterable([1, 2, 3])
    [1, 2, 3]
    >>> as_iterable("my string")
    ("my string", )
    >>> as_iterable({'a': 1})
    ({'a': 1}, )

    """

    if iterable_or_scalar is None:
        return ()
    elif isinstance(iterable_or_scalar, string_types):
        return (iterable_or_scalar,)
    elif hasattr(iterable_or_scalar, "__iter__"):
        return iterable_or_scalar
    else:
        return (iterable_or_scalar,)

# preserve backwards compatibility
_as_iterable = as_iterable

@add_metaclass(ABCMeta)
class JVMHelpers(object):
    """General purpose helpers for the jvm and scala.

    This requires that you already have a live jvm somewhere via py4j

    In order to creaete an instance of this you need to implement methods to provide a

    * jvm : jvm
    * gateway : py4j gateway bridge
    * classloader : the java classloader needed to load your classes.

    """

    def __init__(self):
        self._scala_package_objects = {}
        self._scala_objects = {}

    @abstractproperty
    def jvm(self):
        """Py4J jvm mirror"""
        pass

    @abstractproperty
    def gateway(self):
        """Py4J gateway"""
        pass

    @abstractproperty
    def classloader(self):
        """Java classloader to use to create scala instances"""
        pass

    def import_scala_package_object(self, package_name):
        """Imports a scala package object by name.

        This is done by using reflection on the underlying Java class that is created by Scaa.

        Examples
        --------
        >>> instance.import_scala_package_object("org.apache.spark.sql")

        """
        if package_name not in self._scala_package_objects:
            jpackage = self.classloader.loadClass("{}.package$".format(package_name))
            j_emptyClassArray = self.gateway.gw.new_array(self.gateway.jvm.Class, 0)
            j_emptyObjectArray = self.gateway.new_array(self.gateway.jvm.Object, 0)
            jconst = jpackage.getDeclaredConstructor(j_emptyClassArray)
            jconst.setAccessible(True)
            instance = jconst.newInstance(j_emptyObjectArray)
            self._scala_package_objects[package_name] = instance
        return self._scala_package_objects[package_name]

    def import_scala_object(self, object_name):
        """Imports a scala object by name.

        Scala objects are singletons so this will either import or retrive one from the cache.

        Scala case classes are also both a Class and an Object so if you want to retrieve the Scala
        thing that you can call ``obj.apply(..)``` on you want to use this.
        """
        if object_name not in self._scala_objects:
            jpackage = self.classloader.loadClass("{}$".format(object_name))
            j_emptyClassArray = self.gateway.new_array(self.gateway.jvm.Class, 0)
            j_emptyObjectArray = self.gateway.new_array(self.gateway.jvm.Object, 0)
            jconst = jpackage.getDeclaredConstructor(j_emptyClassArray)
            jconst.setAccessible(True)
            instance = jconst.newInstance(j_emptyObjectArray)
            self._scala_objects[object_name] = instance
        return self._scala_objects[object_name]

    def to_scala_seq(self, list_like):
        """Converts a python list-like object to a scala.collection.immutable.Seq.
        """
        l = as_iterable(list_like)
        converters = self.import_scala_object("scala.collection.JavaConverters")
        # Since py4j already converts a python list to a Java.util.List<> we can make use of the
        # scala converters
        return converters.asScalaBufferConverter(l).asScala().toSeq()

    def to_scala_map(self, dict_like):
        """Converts a python dict to a scala.collection.immutable.Map
        """
        converters = self.import_scala_object("scala.collection.JavaConverters")
        # Since py4j already converts a python list to a Java.util.List<> we can make use of the
        # scala converters
        return converters.mapAsScalaMapConverter(dict_like).asScala()

    def to_scala_list(self, list_like):
        """Converts a python list-like to a scala.collection.List
        """
        return self.to_scala_seq(list_like).toList()

    def get_classtag(self, java_class_name):
        jclassTagO = self.import_scala_object("scala.reflect.ClassTag")
        klass = self.classloader.loadClass(java_class_name)
        return jclassTagO.apply(klass)

    def to_scala_array(self, list_like, java_class_name):
        """Converts a python list-like to a a scala Array

        Parameters
        ----------
        list_like : list
        java_class_name : str
            Java class name to use for the conversion

        Examples
        >>> c.to_scala_array([1,2,3,4], "java.lang.Integer")
        ...

        """
        jseq = self.to_scala_seq(list_like)
        return jseq.toArray(self.get_classtag(java_class_name))

    def to_scala_set(self, set_like):
        """Converts a python set-like to a scala.collection.Set
        """
        converters = self.import_scala_object("scala.collection.JavaConverters")
        return converters.asScalaSetConverter(set_like).asScala().toSet()
