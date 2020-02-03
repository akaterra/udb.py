import pytest

from udb_py.common import InvalidScanOperationValueError
from udb_py.index.udb_rtree_index import UdbRtreeIndex

class UdbRtreeIndexTest(UdbRtreeIndex):
    @property
    def index(self):
        return self._rtree


def test_should_delete():
    i = UdbRtreeIndexTest('a')

    i.insert([1, 2], 123).delete([1, 2], 123)

    assert list(i.index.intersection([1, 2])) == []


def test_should_insert():
    i = UdbRtreeIndexTest('a')

    i.insert([1, 2], 123)

    assert list(i.index.intersection([1, 2])) == [123]


def test_should_insert_by_schema():
    i = UdbRtreeIndexTest('a')

    i.insert_by_schema({'a': [1, 2]}, 123)

    assert list(i.index.intersection([1, 2])) == [123]


def test_should_insert_by_schema_with_default_value():
    i = UdbRtreeIndexTest('a', default_value=[0, 0])

    i.insert_by_schema({'b': [1, 2]}, 123)

    assert list(i.index.intersection([0, 0])) == [123]


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbRtreeIndexTest('a', default_value=lambda key, values: [0, 0])

    i.insert_by_schema({'b': [1, 2]}, 123)

    assert list(i.index.intersection([0, 0])) == [123]


def test_should_upsert():
    i = UdbRtreeIndexTest('a')

    i.insert([1, 2], 123).upsert([1, 2], [3, 4], 123)

    assert list(i.index.intersection([3, 4])) == [123]


def test_should_upsert_deleting_old_key():
    i = UdbRtreeIndexTest('a')

    i.insert([1, 2], 123).upsert([1, 2], [3, 4], 123)

    assert list(i.index.intersection([1, 2])) == []


def test_should_select_by_near():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(2, 2)) == [1, 3, 2]


def test_should_select_by_near_with_max_distance():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(
        2,
        2,
        max_distance=2,
        collection={
            1: {'a': [2, 2]},  # 1
            2: {'a': [0, 0]},
            3: {'a': [3, 3]},  # 3
        },
        limit=None,
    )) == [1, 3]


def test_should_select_by_near_with_min_distance():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(
        2,
        2,
        min_distance=1,
        collection={
            1: {'a': [2, 2]},
            2: {'a': [0, 0]},  # 2
            3: {'a': [3, 3]},  # 3
        },
        limit=None,
    )) == [3, 2]


def test_should_select_by_near_with_limit():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(2, 2, limit=2)) == [1, 3]


def test_should_select_by_near_with_max_distance_with_limit():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(
        2,
        2,
        max_distance=2,
        collection={
            1: {'a': [2, 2]},  # 1
            2: {'a': [0, 0]},
            3: {'a': [3, 3]},
        },
        limit=1,
    )) == [1]


def test_should_select_by_near_with_min_distance_with_limit():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_near(
        2,
        2,
        min_distance=1,
        collection={
            1: {'a': [2, 2]},
            2: {'a': [0, 0]},
            3: {'a': [3, 3]},  # 3
        },
        limit=1,
    )) == [3]


def test_should_select_by_intersection():
    i = UdbRtreeIndex('a')

    i.insert([2, 2], 1).insert([0, 0], 2).insert([3, 3], 3)

    assert list(i.search_by_intersection(1, 1, 3, 3)) == [1, 3]
