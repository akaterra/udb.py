import pytest

from udb_py.common import *
from udb_py.index.udb_btree_multivalued_index import UdbBtreeMultivaluedIndex


class UdbBtreeMultivaluedIndexTest(UdbBtreeMultivaluedIndex):
    @property
    def index(self):
        return self._btree


def test_should_delete():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).delete('123', 123)

    assert i.index.get('123', 123) == {333}


def test_should_insert():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333)

    assert i.index.get('123') == {123, 333}


def test_should_insert_by_schema():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert_by_schema({'a': 1, 'b': 2, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 2, 3]))) == {123}


def test_should_insert_by_schema_with_default_value():
    i = UdbBtreeMultivaluedIndexTest((('a', required), ('b', 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == {123}


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbBtreeMultivaluedIndexTest((('a', required), ('b', lambda key, values: 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == {123}


def test_should_upsert():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 111).upsert('123', '321', 123)

    assert i.index.get('321') == {123}


def test_should_upsert_deleting_old_key():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 111).upsert('123', '321', 123)

    assert i.index.get('123') == {111}


def test_should_search_by_key():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key('123')) == [123, 333]


def test_should_search_by_key_in():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_in(['123', '111'])) == [123, 333, 111]


def test_should_search_by_key_prefix():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_prefix('1')) == [111, 123, 333]


def test_should_search_by_key_range():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('12', '33')) == [123, 333, 321]


def test_should_search_by_key_range_excluding_min():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', gte_excluded=True)) == [321, 333]


def test_should_search_by_key_range_excluding_max():
    i = UdbBtreeMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', lte_excluded=True)) == [123, 321]
