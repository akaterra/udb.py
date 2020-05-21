import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeIndex


@pytest.mark.udb
def test_should_delete_all():
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

    delete_count = udb.delete()

    assert delete_count == 3
    assert list(udb.select()) == []
    assert len(udb.indexes['a']) == 0
    assert len(udb.indexes['ab']) == 0
    assert len(udb.indexes['b']) == 0


@pytest.mark.udb
def test_should_delete_by_query():
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

    delete_count = udb.delete({'a': 2})

    assert delete_count == 1
    assert list(udb.select()) == [{'a': 1, 'b': 1, '__rev__': 0}, {'a': 3, 'b': 3, '__rev__': 2}]
    assert len(udb.indexes['a']) == 2
    assert len(udb.indexes['ab']) == 2
    assert len(udb.indexes['b']) == 2


@pytest.mark.udb
def test_should_delete_rotating_delete_buffer():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
    })

    for i in range(0, 10000):
        udb.insert({'a': i})

    delete_count = udb.delete({'a': {'$lte': 4999}})

    assert delete_count == 5000

    for i, r in enumerate(udb.select({})):
        assert r['a'] == i + 5000
