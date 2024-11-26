import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeBaseIndex, UdbBtreeIndex


def test_should_select_by_full_covered_query():
    udb = Udb({
        'a': UdbBtreeBaseIndex(['a']),
        'ab': UdbBtreeBaseIndex(['a', 'b']),
        'b': UdbBtreeBaseIndex(['b']),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 2, 'c': 2}
    c = {'a': 3, 'b': 3, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 2, 'b': 2}))

    assert len(records) == 1
    assert records[0] == {'a': 2, 'b': 2, 'c': 2, '__rev__': 1}

    records = list(udb.select({'a': 0, 'b': 0}))

    assert len(records) == 0


def test_should_select_by_full_covered_query_using_functional_index():
    udb = Udb({
        'a': UdbBtreeBaseIndex(['a']),
        'ab': UdbBtreeIndex({'a': 'a', '$size': lambda key, values: len(values['c']) if isinstance(values['c'], list) else 0}),
        'b': UdbBtreeBaseIndex(['b']),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 1, 'b': 2, 'c': [2, 2]}
    c = {'a': 3, 'b': 3, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 1, '$size': 2}))

    assert len(records) == 1
    assert records[0] == {'a': 1, 'b': 2, 'c': [2, 2], '__rev__': 1}

    records = list(udb.select({'a': 3, '$size': 2}))

    assert len(records) == 0


def test_should_select_by_partially_covered_query():
    udb = Udb({
        'a': UdbBtreeBaseIndex(['a']),
        'ab': UdbBtreeBaseIndex(['a', 'c']),
        'b': UdbBtreeBaseIndex(['b']),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 2, 'b': 2}))

    assert len(records) == 1
    assert records[0] == {'a': 2, 'b': 2, '__rev__': 1}

    records = list(udb.select({'a': 2, 'b': 2, 'c': 3}))

    assert len(records) == 0


def test_should_select_by_subset_using_indexes():
    udb = Udb({
        'a': UdbBtreeBaseIndex(['a']),
        'ab': UdbBtreeBaseIndex(['a', 'b']),
        'b': UdbBtreeBaseIndex(['b']),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 1, 'b': 2, 'c': 2}
    c = {'a': 3, 'b': 3, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 1}, offset=1, limit=5, use_indexes=['ab']))

    assert len(records) == 1
    assert records[0] == {'a': 1, 'b': 2, 'c': 2, '__rev__': 1}

    records = list(udb.select({'a': 1}, offset=9, limit=5, use_indexes=['ab']))

    assert len(records) == 0
