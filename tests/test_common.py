from __future__ import print_function, absolute_import

from spylon.common import as_iterable
import pytest

__author__ = 'mniekerk'


@pytest.mark.parametrize("test_input,expected", [
    (4, (4,)),
    (None, ()),
    ("string", ('string',)),
    ([1, 2, 3], [1, 2, 3]),
    ({1, 2, 3}, {1, 2, 3})
])
def test_as_iterable(test_input, expected):
    assert as_iterable(test_input) == expected
