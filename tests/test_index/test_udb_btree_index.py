import pytest

from udbpy.common import *
from udbpy.index.udb_btree_index import UdbBtreeIndex


class UdbBtreeIndexTest(UdbBtreeIndex):
    @property
    def index(self):
        return self._btree


def test_should_delete():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).delete('123')

    assert i.index.get('123', 0) == 0


def test_should_insert():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123)

    assert i.index.get('123') == 123


def test_should_insert_by_schema():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert_by_schema({'a': 1, 'b': 2, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 2, 3]))) == 123


def test_should_insert_by_schema_with_default_value():
    i = UdbBtreeIndexTest((('a', required), ('b', 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbBtreeIndexTest((('a', required), ('b', lambda key, values: 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_upsert():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('321') == 123


def test_should_upsert_deleting_old_key():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('123', None) is None


def test_should_search_by_key():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key('123')) == [123]


def test_should_search_by_key_in():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_in(['123', '111'])) == [123, 111]


def test_should_search_by_key_prefix():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_prefix('1')) == [111, 123]


def test_should_search_by_key_range():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('12', '33')) == [123, 321]


def test_should_search_by_key_range_excluding_min():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', gte_excluded=True)) == [321, 333]


def test_should_search_by_key_range_excluding_max():
    i = UdbBtreeIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', lte_excluded=True)) == [123, 321]
