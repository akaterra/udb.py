import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeIndex, UdbClusterIndex


def test_should_select_using_index_and_inner_index(mocker):
    search_by_key_eq_spy = mocker.spy(UdbBtreeIndex, 'search_by_key_eq')
    udb_index_a = UdbBtreeIndex(['a'])
    udb_index_ab = UdbBtreeIndex(['a', 'b'])
    udb_index_ab_c = UdbBtreeIndex(['c'])
    udb_index_b = UdbBtreeIndex(['b'])
    udb = Udb({
        'a': UdbClusterIndex(udb_index_a),
        'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
        'b': UdbClusterIndex(udb_index_b),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 1, 'c': 2}
    c = {'a': 1, 'b': 1, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 1, 'b': 1, 'c': 3}))

    assert records == [c]
    assert search_by_key_eq_spy.call_count == 2


def test_should_select_using_index_and_not_inner(mocker):
    search_by_key_eq_spy = mocker.spy(UdbBtreeIndex, 'search_by_key_eq')
    udb_index_a = UdbBtreeIndex(['a'])
    udb_index_ab = UdbBtreeIndex(['a', 'b'])
    udb_index_ab_c = UdbBtreeIndex(['d'])
    udb_index_b = UdbBtreeIndex(['b'])
    udb = Udb({
        'a': UdbClusterIndex(udb_index_a),
        'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
        'b': UdbClusterIndex(udb_index_b),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 1, 'c': 2}
    c = {'a': 1, 'b': 1, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 1, 'b': 1, 'c': 3}))

    assert records == [c]
    assert search_by_key_eq_spy.call_count == 1


def test_should_select_not_using_index(mocker):
    search_by_key_eq_spy = mocker.spy(UdbBtreeIndex, 'search_by_key_eq')
    udb_index_a = UdbBtreeIndex(['A'])
    udb_index_ab = UdbBtreeIndex(['A', 'b'])
    udb_index_ab_c = UdbBtreeIndex(['c'])
    udb_index_b = UdbBtreeIndex(['B'])
    udb = Udb({
        'a': UdbClusterIndex(udb_index_a),
        'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
        'b': UdbClusterIndex(udb_index_b),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 1, 'c': 2}
    c = {'a': 1, 'b': 1, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 1, 'b': 1, 'c': 3}))

    assert records == [c]
    assert search_by_key_eq_spy.call_count == 0
