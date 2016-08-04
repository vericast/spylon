from __future__ import print_function, absolute_import

import pytest
from py4j.java_gateway import JavaGateway
from spylon.simple import SimpleJVMHelpers

__author__ = 'mniekerk'


@pytest.fixture(scope="module")
def jvm_gateway(request):
    gw = JavaGateway.launch_gateway()
    assert isinstance(gw, JavaGateway)
    request.addfinalizer(gw.shutdown)
    return gw


def test_simple_jvm_helper(jvm_gateway):
    j = SimpleJVMHelpers(jvm_gateway)
    assert j.classloader
    assert j.jvm