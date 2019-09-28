import pytest

from udb.common import *
from udb.index.udb_hash_multivalued_index import UdbHashMultivaluedIndex


class UdbHashMultivaluedIndexTest(UdbHashMultivaluedIndex):
    @property
    def index(self):
        return self._hash


def test_should_delete():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).delete('123', 123)

    assert i.index.get('123', 123) == {333}


def test_should_insert():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333)

    assert i.index.get('123') == {123, 333}


def test_should_insert_by_schema():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert_by_schema({'a': 1, 'b': 2, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 2, 3]))) == {123}


def test_should_insert_by_schema_with_default_value():
    i = UdbHashMultivaluedIndexTest((('a', required), ('b', 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == {123}


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbHashMultivaluedIndexTest((('a', required), ('b', lambda key, values: 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == {123}


def test_should_upsert():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 111).upsert('123', '321', 123)

    assert i.index.get('321') == {123}


def test_should_upsert_deleting_old_key():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 111).upsert('123', '321', 123)

    assert i.index.get('123') == {111}


def test_should_search_by_key():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key('123')) == [123, 333]


def test_should_search_by_key_in():
    i = UdbHashMultivaluedIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('123', 123).insert('123', 333).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_in(['123', '111'])) == [123, 333, 111]
