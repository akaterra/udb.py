import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeBaseIndex
from udb_py.udb_view import UdbView


def test_should_select_by_view_defined_condition():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 2}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 3})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 4, 'b': 3})

    records = list(udb_view.select())

    assert records == [{
        '__rev__': 1, 'a': 2, 'b': 3,
    }, {
        '__rev__': 3, 'a': 4, 'b': 3,
    }]


def test_should_select_by_view_defined_and_provided_conditions():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 1}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 3})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 4, 'b': 3})

    records = list(udb_view.select({'a': {'$gte': 2}}))

    assert records == [{
        '__rev__': 1, 'a': 2, 'b': 3,
    }, {
        '__rev__': 3, 'a': 4, 'b': 3,
    }]


def test_should_not_select_deleted_record():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 2}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 3})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 4, 'b': 3})

    udb.delete({'a': 2, 'b': 3})

    records = list(udb_view.select())

    assert records == [{
        '__rev__': 3, 'a': 4, 'b': 3,
    }]


def test_should_not_select_inserted_record_is_not_matching_condition():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 2}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 2, 'b': 4})
    udb.insert({'a': 4, 'b': 3})

    udb.insert({'a': 5, 'b': 2})

    records = list(udb_view.select())

    assert records == [{
        '__rev__': 3, 'a': 4, 'b': 3,
    }]


def test_should_select_updated_record():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 2}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 2, 'b': 4})
    udb.insert({'a': 4, 'b': 1})

    udb.update({'b': 3}, {'a': 2, 'b': 2})

    records = list(udb_view.select())

    assert records == [{
        '__rev__': 5, 'a': 2, 'b': 3,
    }]


def test_not_should_select_updated_record_is_not_matching_condition():
    udb = Udb()
    udb_view = UdbView(udb, {'a': {'$gte': 2}, 'b': 3})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 2, 'b': 4})
    udb.insert({'a': 4, 'b': 1})

    udb.update({'b': 3}, {'a': 1, 'b': 2})

    records = list(udb_view.select())

    assert records == []


def test_should_select_using_index_of_linked_db():
    class UdbIndexA(UdbBtreeBaseIndex):
        is_called = False

        def search_by_key_eq(self, key):
            self.is_called = True

            yield next(UdbBtreeBaseIndex.search_by_key_eq(self, key))

    udb_index_a = UdbIndexA(['a'])
    udb = Udb({'a': udb_index_a})
    udb_view = UdbView(udb, {'a': 4})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 2, 'b': 4})
    udb.insert({'a': 4, 'b': 1})

    records = list(udb_view.select())

    assert udb_index_a.is_called is True
    assert records == [{
        '__rev__': 3, 'a': 4, 'b': 1,
    }]


def test_should_select_self_defined_index():
    class UdbIndexA(UdbBtreeBaseIndex):
        is_called = False

        def search_by_key_eq(self, key):
            self.is_called = True

            yield next(UdbBtreeBaseIndex.search_by_key_eq(self, key))

    udb_index_a = UdbIndexA(['a'])
    udb = Udb()
    udb_view = UdbView(udb, {'a': 4}, {'a': udb_index_a})

    udb.insert({'a': 1, 'b': 2})
    udb.insert({'a': 2, 'b': 2})
    udb.insert({'a': 2, 'b': 4})
    udb.insert({'a': 4, 'b': 1})

    records = list(udb_view.select())

    assert udb_index_a.is_called is True
    assert records == [{
        '__rev__': 3, 'a': 4, 'b': 1,
    }]
