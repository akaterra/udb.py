import pytest

from udb_py.common import *
from udb_py.index.udb_btree_base_index import UdbBtreeEmbeddedBaseIndex


class UdbBtreeEmbeddedIndexTest(UdbBtreeEmbeddedBaseIndex):
    @property
    def index(self):
        return self._btree


def test_should_delete():
    i = UdbBtreeEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).delete(['2', '3'])

    assert i.index.get('2', None) is None and i.index.get('3', None) is None


def test_should_insert():
    i = UdbBtreeEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123)

    assert i.index.get('1') == 123 and i.index.get('2') == 123 and i.index.get('3') == 123


def test_should_upsert():
    i = UdbBtreeEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).upsert(['2', '3'], ['4', '5'], 123)

    assert i.index.get('1') == 123 and i.index.get('4') == 123 and i.index.get('5') == 123


def test_should_upsert_deleting_old_key():
    i = UdbBtreeEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).upsert(['2', '3'], ['4', '5'], 123)

    assert i.index.get('2', None) is None and i.index.get('3', None) is None
