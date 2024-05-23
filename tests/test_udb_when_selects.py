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


def test_should_select_using_seq_scan_by_eq_operator():
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

    records = list(udb.select({'a': {'$eq': 1}}))

    assert records == [e]


def test_should_select_using_seq_scan_by_ne_operator():
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

    records = list(udb.select({'a': {'$ne': 1}}))

    assert records == [a, b, c, d, f]


def test_should_select_using_seq_scan_by_in_operator():
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

    records = list(udb.select({'a': {'$in': [False, 1]}}))

    assert records == [b, e]


def test_should_select_using_seq_scan_by_nin_operator():
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

    records = list(udb.select({'a': {'$nin': [False, 1]}}))

    assert records == [a, c ,d, f]


def test_should_select_using_seq_scan_by_like_operator():
    udb = Udb()

    a = {'a': 'abbbcd', '__rev__': 0}
    b = {'a': 'abbbdd', '__rev__': 1}
    c = {'a': 'abcd', '__rev__': 2}
    d = {'a': 'abcdd', '__rev__': 3}
    e = {'a': 'abc', '__rev__': 4}
    f = {'a': 'Aabcd', '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$like': 'a%c_'}}))

    assert records == [a, c]


def test_should_select_using_seq_scan_by_intersection_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': (1, 1), '__rev__': 1}
    c = {'a': (1, 3), '__rev__': 2}
    d = {'a': (3, 1), '__rev__': 3}
    e = {'a': (3, 3), '__rev__': 4}
    f = {'a': (2, 2), '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$intersection': {'xMin': 2, 'xMax': 4, 'yMin': 0, 'yMax': 2}}}))

    assert records == [d, f]


def test_should_select_using_seq_scan_by_near_operator():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': (1, 1), '__rev__': 1}
    c = {'a': (1, 3), '__rev__': 2}
    d = {'a': (3, 1), '__rev__': 3}
    e = {'a': (3, 3), '__rev__': 4}
    f = {'a': (2, 2), '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$near': {'x': 3, 'y': 3}}}))

    assert records == [e, f, c, d, b]


def test_should_select_using_seq_scan_by_near_operator_with_min_distance():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': (1, 1), '__rev__': 1}
    c = {'a': (1, 3), '__rev__': 2}
    d = {'a': (3, 1), '__rev__': 3}
    e = {'a': (3, 3), '__rev__': 4}
    f = {'a': (2, 2), '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$near': {'x': 3, 'y': 3, 'minDistance': 1}}}))

    assert records == [f, c, d, b]


def test_should_select_using_seq_scan_by_near_operator_with_max_distance():
    udb = Udb()

    a = {'a': None, '__rev__': 0}
    b = {'a': (1, 1), '__rev__': 1}
    c = {'a': (1, 3), '__rev__': 2}
    d = {'a': (3, 1), '__rev__': 3}
    e = {'a': (3, 3), '__rev__': 4}
    f = {'a': (2, 2), '__rev__': 5}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)
    udb.insert(d)
    udb.insert(e)
    udb.insert(f)

    records = list(udb.select({'a': {'$near': {'x': 3, 'y': 3, 'maxDistance': 2}}}))

    assert records == [e, f, c, d]


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


def test_should_select_using_btree_index_by_range():
    udb = Udb(indexes={'a': UdbBtreeBaseIndex(['a', 'b', 'c'])})

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
