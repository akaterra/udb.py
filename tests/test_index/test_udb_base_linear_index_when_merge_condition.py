import pytest

from udb_py.common import FieldRequiredError, REQUIRED, type_formatter_iter
from udb_py.index.udb_base_linear_index import UdbBaseLinearIndex


def test_should_override_gt_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$gt': 2}}, {'b': {'$gt': 1}, 'c': 4})

    assert q == {'a': 1, 'b': {'$gt': 2}, 'c': 4}


def test_should_not_override_gt_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$gt': 2}}, {'b': {'$gt': 3}, 'c': 4})

    assert q == {'a': 1, 'b': {'$gt': 3}, 'c': 4}


def test_should_override_gte_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$gte': 2}}, {'b': {'$gte': 1}, 'c': 4})

    assert q == {'a': 1, 'b': {'$gte': 2}, 'c': 4}


def test_should_not_override_gte_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$gte': 2}}, {'b': {'$gte': 3}, 'c': 4})

    assert q == {'a': 1, 'b': {'$gte': 3}, 'c': 4}


def test_should_override_lt_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$lt': 1}}, {'b': {'$lt': 2}, 'c': 4})

    assert q == {'a': 1, 'b': {'$lt': 2}, 'c': 4}


def test_should_not_override_lt_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$lt': 3}}, {'b': {'$lt': 2}, 'c': 4})

    assert q == {'a': 1, 'b': {'$lt': 3}, 'c': 4}


def test_should_override_lte_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$lte': 1}}, {'b': {'$lte': 2}, 'c': 4})

    assert q == {'a': 1, 'b': {'$lte': 2}, 'c': 4}


def test_should_not_override_lte_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': {'$lte': 3}}, {'b': {'$lte': 2}, 'c': 4})

    assert q == {'a': 1, 'b': {'$lte': 3}, 'c': 4}


def test_should_override_primitive_value():
    q = UdbBaseLinearIndex.merge_condition({'a': 1, 'b': 2}, {'b': 3, 'c': 4})

    assert q == {'a': 1, 'b': 3, 'c': 4}
