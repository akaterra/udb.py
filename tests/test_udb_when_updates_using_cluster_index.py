import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeIndex, UdbClusterIndex


# def test_should_update_all():
#     udb_index_a = UdbBtreeIndex(['a'])
#     udb_index_ab = UdbBtreeIndex(['a', 'b'])
#     udb_index_ab_c = UdbBtreeIndex(['c'])
#     udb_index_b = UdbBtreeIndex(['b'])
#     udb = Udb({
#         'a': UdbClusterIndex(udb_index_a),
#         'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
#         'b': UdbClusterIndex(udb_index_b),
#     })
#
#     a = {'a': 1, 'b': 1, 'c': 1}
#     b = {'a': 2, 'b': 1, 'c': 2}
#     c = {'a': 1, 'b': 1, 'c': 3}
#
#     udb.insert(a)
#     udb.insert(b)
#     udb.insert(c)
#
#     update_count = udb.update({'a': 1})
#
#     assert update_count == 3
#     assert list(udb.select({'a': 1, 'b': 1})) == [{
#         'a': 1, 'b': 1, 'c': 1, '__rev__': 4,
#     }, {
#         'a': 1, 'b': 1, 'c': 2, '__rev__': 4,
#     }, {
#         'a': 1, 'b': 1, 'c': 3, '__rev__': 4,
#     }]
#     assert len(udb.indexes['a']) == 3
#     assert len(udb.indexes['ab']) == 3
#     assert len(udb.indexes['b']) == 3


def test_should_update_by_query_on_outer_index():
    udb_index_a = UdbBtreeIndex(['a'])
    udb_index_ab = UdbBtreeIndex(['a', 'b'])
    udb_index_ab_c = UdbBtreeIndex(['c'])
    udb_index_b = UdbBtreeIndex(['b'])
    udb = Udb({
        # 'a': UdbClusterIndex(udb_index_a),
        'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
        # 'b': UdbClusterIndex(udb_index_b),
    })

    a = {'a': 1, 'b': 1, 'c': 1}
    b = {'a': 2, 'b': 1, 'c': 2}
    c = {'a': 1, 'b': 1, 'c': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    update_count = udb.update({'a': 1}, {'a': 2})

    assert update_count == 1
    assert list(udb.select({'a': 1, 'b': 1})) == [{
        'a': 1, 'b': 1, 'c': 1, '__rev__': 0,
    }, {
        'a': 1, 'b': 1, 'c': 2, '__rev__': 4,
    }, {
        'a': 1, 'b': 1, 'c': 3, '__rev__': 2,
    }]
    # assert len(udb.indexes['a']) == 3
    assert len(udb.indexes['ab']) == 3
    # assert len(udb.indexes['b']) == 3


# def test_should_update_by_query_on_outer_index_creating_new_cluster():
#     udb_index_a = UdbBtreeIndex(['a'])
#     udb_index_ab = UdbBtreeIndex(['a', 'b'])
#     udb_index_ab_c = UdbBtreeIndex(['c'])
#     udb_index_b = UdbBtreeIndex(['b'])
#     udb = Udb({
#         # 'a': UdbClusterIndex(udb_index_a),
#         'ab': UdbClusterIndex(udb_index_ab, udb_index_ab_c),
#         # 'b': UdbClusterIndex(udb_index_b),
#     })
#
#     a = {'a': 1, 'b': 1, 'c': 1}
#     b = {'a': 2, 'b': 1, 'c': 2}
#     c = {'a': 1, 'b': 1, 'c': 3}
#
#     udb.insert(a)
#     udb.insert(b)
#     udb.insert(c)
#
#     update_count = udb.update({'a': 3}, {'a': 2})
#
#     assert update_count == 1
#     assert list(udb.select({'a': 1, 'b': 1})) == [{
#         'a': 1, 'b': 1, 'c': 1, '__rev__': 0,
#     }, {
#         'a': 1, 'b': 1, 'c': 3, '__rev__': 2,
#     }]
#     assert list(udb.select({'a': 3, 'b': 1})) == [{
#         'a': 3, 'b': 1, 'c': 2, '__rev__': 4,
#     }]
#     # assert len(udb.indexes['a']) == 3
#     assert len(udb.indexes['ab']) == 3
#     # assert len(udb.indexes['b']) == 3
