import pytest

from udb_py.common import auto_id, current_timestamp, ConstraintError
from udb_py.udb import Udb, UdbBtreeIndex, UdbBtreeUniqIndex


@pytest.mark.udb
def test_should_insert():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    assert list(udb.select()) == [
        {'a': 1, 'b': 1, '__rev__': 0},
        {'a': 2, 'b': 2, '__rev__': 1},
        {'a': 3, 'b': 3, '__rev__': 2},
    ]
    assert len(udb.indexes['a']) == 3
    assert len(udb.indexes['ab']) == 3
    assert len(udb.indexes['b']) == 3


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


@pytest.mark.udb
def test_should_raise_conflict_error_on_uniq_index():
    udb = Udb({
        'a': UdbBtreeUniqIndex(['a']),
        'ab': UdbBtreeUniqIndex(['a', 'b']),
        'b': UdbBtreeUniqIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    with pytest.raises(Exception) as excinfo:
        udb.insert({'a': 2})

    assert isinstance(excinfo.value, ConstraintError) is True
