import pytest

from udb_py.common import *
from udb_py.index.udb_hash_uniq_index import UdbHashUniqIndex, ConstraintError


class UdbHashUniqIndexTest(UdbHashUniqIndex):
    @property
    def index(self):
        return self._hash


def test_should_delete():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).delete('123')

    assert i.index.get('123', 0) == 0


def test_should_insert():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123)

    assert i.index.get('123') == 123


def test_should_not_insert_on_duplicate_key_and_raise_constraint_error():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    with pytest.raises(ConstraintError):
        i.insert('123', 123).insert('123', 321)

    assert i.index.get('123') == 123


def test_should_insert_by_schema():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert_by_schema({'a': 1, 'b': 2, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 2, 3]))) == 123


def test_should_insert_by_schema_with_default_value():
    i = UdbHashUniqIndexTest((('a', required), ('b', 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbHashUniqIndexTest((('a', required), ('b', lambda key, values: 1), ('c', required)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_upsert():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('321') == 123


def test_should_not_upsert_on_duplicate_key_and_raise_exception():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    with pytest.raises(ConstraintError):
        i.insert('123', 123).insert('321', 321).upsert('321', '123', 321)


def test_should_upsert_deleting_old_key():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('123', None) is None


def test_should_search_by_key():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key('123')) == [123]


def test_should_search_by_key_in():
    i = UdbHashUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_in(['123', '111'])) == [123, 111]
