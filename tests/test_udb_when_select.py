import pytest

from udb.common import *
from udb.udb import Udb, UdbBtreeIndex


@pytest.mark.udb
def test_should_select_by_full_covered_query():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
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


@pytest.mark.udb
def test_should_select_by_partially_covered_query():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 2, 'c': 2}
    c = {'a': 3, 'b': 3, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    records = list(udb.select({'a': 2, 'b': 2, 'c': 2}))

    assert len(records) == 1
    assert records[0] == {'a': 2, 'b': 2, 'c': 2, '__rev__': 1}

    records = list(udb.select({'a': 2, 'b': 2, 'c': 3}))

    assert len(records) == 0


@pytest.mark.udb
def test_should_select_by_subset_using_indexes():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
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


@pytest.mark.udb
def test_should_select_using_seq_scan_by_gt_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': False, '__rev__': 1}
    c = {'a': True, '__rev__': 2}
    d = {'a': -1, '__rev__': 3}
    e = {'a': 1, '__rev__': 4}
    f = {'a': '1', '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$gt': None}}))

    assert records == [b, c, d, e, f]


@pytest.mark.udb
def test_should_select_using_seq_scan_by_gte_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': False, '__rev__': 1}
    c = {'a': True, '__rev__': 2}
    d = {'a': -1, '__rev__': 3}
    e = {'a': 1, '__rev__': 4}
    f = {'a': '1', '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$gte': False}}))

    assert records == [b, c, d, e, f]


@pytest.mark.udb
def test_should_select_using_seq_scan_by_lt_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': False, '__rev__': 1}
    c = {'a': True, '__rev__': 2}
    d = {'a': -1, '__rev__': 3}
    e = {'a': 1, '__rev__': 4}
    f = {'a': '1', '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$lt': '1'}}))

    assert records == [a, b, c, d, e]


@pytest.mark.udb
def test_should_select_using_seq_scan_by_lte_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': False, '__rev__': 1}
    c = {'a': True, '__rev__': 2}
    d = {'a': -1, '__rev__': 3}
    e = {'a': 1, '__rev__': 4}
    f = {'a': '1', '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$lte': 1}}))

    assert records == [a, b, c, d, e]


@pytest.mark.udb
def test_should_select_by_range():
    udb = Udb()

    udb.insert({'a': 1, 'b': 0, 'c': 0})
    udb.insert({'a': 1, 'b': 1, 'c': 1})
    udb.insert({'a': 1, 'b': 1, 'c': 2})
    udb.insert({'a': 1, 'b': 2, 'c': 3})

    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$gt': 1}})) == [
        {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$gte': 1}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1}, {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$lt': 2}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$lte': 2}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1}, {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]


@pytest.mark.udb
def test_should_select_using_btree_index_by_range():
    udb = Udb(indexes={'a': UdbBtreeIndex(['a', 'b', 'c'])})

    udb.insert({'a': 1, 'b': 0, 'c': 0})
    udb.insert({'a': 1, 'b': 1, 'c': 1})
    udb.insert({'a': 1, 'b': 1, 'c': 2})
    udb.insert({'a': 1, 'b': 2, 'c': 3})

    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$gt': 1}})) == [
        {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$gte': 1}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1}, {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$lt': 2}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1},
    ]
    assert list(udb.select({'a': 1, 'b': 1, 'c': {'$lte': 2}})) == [
        {'__rev__': 1, 'a': 1, 'b': 1, 'c': 1}, {'__rev__': 2, 'a': 1, 'b': 1, 'c': 2},
    ]


@pytest.mark.udb
def test_should_select_using_sort():
    udb = Udb()

    a = {'a': 6, '__rev__': 0}
    b = {'a': 1, '__rev__': 1}
    c = {'a': 5, '__rev__': 2}
    d = {'a': 2, '__rev__': 3}
    e = {'a': 4, '__rev__': 4}
    f = {'a': 3, '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select(sort='a'))

    assert records == [b, d, f, e, c, a]


@pytest.mark.udb
def test_should_select_using_reverse_sort():
    udb = Udb()

    a = {'a': 6, '__rev__': 0}
    b = {'a': 1, '__rev__': 1}
    c = {'a': 5, '__rev__': 2}
    d = {'a': 2, '__rev__': 3}
    e = {'a': 4, '__rev__': 4}
    f = {'a': 3, '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select(sort='-a'))

    assert records == [a, c, e, f, d, b]
