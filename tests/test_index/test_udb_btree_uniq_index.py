import pytest

from udb_py.common import *
from udb_py.index.udb_btree_uniq_index import UdbBtreeUniqBaseIndex, ConstraintError


class UdbBtreeUniqIndexTest(UdbBtreeUniqBaseIndex):
    @property
    def index(self):
        return self._btree


def test_should_delete():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).delete('123')

    assert i.index.get('123', 0) == 0


def test_should_insert():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123)

    assert i.index.get('123') == 123


def test_should_not_insert_on_duplicate_key_and_raise_constraint_error():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    with pytest.raises(ConstraintError):
        i.insert('123', 123).insert('123', 321)

    assert i.index.get('123') == 123


def test_should_insert_by_schema():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert_by_schema({'a': 1, 'b': 2, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 2, 3]))) == 123


def test_should_insert_by_schema_with_default_value():
    i = UdbBtreeUniqIndexTest((('a', REQUIRED), ('b', 1), ('c', REQUIRED)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbBtreeUniqIndexTest((('a', REQUIRED), ('b', lambda key, values: 1), ('c', REQUIRED)))

    i.insert_by_schema({'a': 1, 'c': 3}, 123)

    assert i.index.get(''.join(type_formatter_iter([1, 1, 3]))) == 123


def test_should_upsert():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('321') == 123


def test_should_not_upsert_on_duplicate_key_and_raise_exception():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    with pytest.raises(ConstraintError):
        i.insert('123', 123).insert('321', 321).upsert('321', '123', 321)


def test_should_upsert_deleting_old_key():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).upsert('123', '321', 123)

    assert i.index.get('123', None) is None


def test_should_search_by_key():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_eq('123')) == [123]


def test_should_search_by_key_in():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_in(['123', '111'])) == [123, 111]


def test_should_search_by_key_prefix():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_prefix('1')) == [111, 123]


def test_should_search_by_key_range():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('12', '33')) == [123, 321]


def test_should_search_by_key_range_excluding_min():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', gte_excluded=True)) == [321, 333]


def test_should_search_by_key_range_excluding_max():
    i = UdbBtreeUniqIndexTest(['a', 'b', 'c'])

    i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

    assert list(i.search_by_key_range('123', '333', lte_excluded=True)) == [123, 321]
