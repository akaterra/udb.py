import pytest

from udb_py.common import auto_id, current_timestamp, ConstraintError
from udb_py.udb import Udb, UdbBtreeBaseIndex, UdbBtreeUniqIndex


def test_should_insert_with_default_value():
    udb = Udb(schema={'b': 2})

    a = {'a': 1, 'c': 1}

    udb.insert(a)

    assert list(udb.select()) == [{'a': 1, 'b': 2, 'c': 1, '__rev__': 0}]


def test_should_insert_with_initial_value_instead_of_default():
    udb = Udb(schema={'b': 2})

    a = {'a': 1, 'b': 3, 'c': 1}

    udb.insert(a)

    assert list(udb.select()) == [{'a': 1, 'b': 3, 'c': 1, '__rev__': 0}]


def test_should_insert_with_default_value_as_callable():
    udb = Udb(schema={'b': lambda key, values: 2})

    a = {'a': 1, 'c': 1}

    udb.insert(a)

    assert list(udb.select()) == [{'a': 1, 'b': 2, 'c': 1, '__rev__': 0}]


def test_should_insert_with_default_value_as_auto_id():
    udb = Udb(schema={'b': auto_id()})

    a = {'a': 1, 'c': 1}

    udb.insert(a)

    assert list(udb.select()) == [{'a': 1, 'b': a['b'], 'c': 1, '__rev__': 0}]


def test_should_insert_with_default_value_as_current_timestamp():
    udb = Udb(schema={'b': current_timestamp()})

    a = {'a': 1, 'c': 1}

    udb.insert(a)

    assert list(udb.select()) == [{'a': 1, 'b': a['b'], 'c': 1, '__rev__': 0}]
