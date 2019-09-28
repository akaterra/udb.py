import pytest

from udb.common import *
from udb.index.udb_hash_index import UdbHashEmbeddedIndex


class UdbHashEmbeddedIndexTest(UdbHashEmbeddedIndex):
    @property
    def index(self):
        return self._hash


def test_should_delete():
    i = UdbHashEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).delete(['2', '3'])

    assert i.index.get('2', None) is None and i.index.get('3', None) is None


def test_should_insert():
    i = UdbHashEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123)

    assert i.index.get('1') == 123 and i.index.get('2') == 123 and i.index.get('3') == 123


def test_should_upsert():
    i = UdbHashEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).upsert(['2', '3'], ['4', '5'], 123)

    assert i.index.get('1') == 123 and i.index.get('4') == 123 and i.index.get('5') == 123


def test_should_upsert_deleting_old_key():
    i = UdbHashEmbeddedIndexTest(['a', 'b', 'c'])

    i.insert(['1', '2', '3'], 123).upsert(['2', '3'], ['4', '5'], 123)

    assert i.index.get('2', None) is None and i.index.get('3', None) is None
