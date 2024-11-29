import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeIndex, UdbClusterIndex


def test_should_select_using_index_and_inner_index():
    class UdbTestBtreeIndex(UdbBtreeIndex):
        is_called = False

        def clone(self):
            return UdbTestBtreeIndex(self.schema, self.name).safe(self._safe)

        def search_by_key_eq(self, key):
            self.is_called = True

            return UdbBtreeIndex.search_by_key_eq(self, key)

    udb_index_a = UdbTestBtreeIndex(['a'])
    udb_index_ab = UdbTestBtreeIndex(['a', 'b'])
    udb_index_ab_c = UdbTestBtreeIndex(['c'])
    udb_index_b = UdbTestBtreeIndex(['b'])
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
    assert udb_index_ab.is_called is True
    assert udb_index_ab_c.is_called is True
