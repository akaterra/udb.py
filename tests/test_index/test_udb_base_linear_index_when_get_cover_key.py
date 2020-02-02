import pytest

from udb_py.common import FieldRequiredError, required, type_formatter_iter
from udb_py.index.udb_base_linear_index import UdbBaseLinearIndex


def test_should_get_cover_key_on_full_covered_data():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])
    d = {'a': 1, 'b': 2, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_not_get_cover_key_on_not_covered_data():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) is None


def test_should_get_cover_key_on_not_covered_data_with_default_value():
    i = UdbBaseLinearIndex((('a', required), ('b', 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_on_not_covered_data_with_default_value_as_callable():
    i = UdbBaseLinearIndex((('a', required), ('b', lambda key, values: 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_on_not_covered_data_as_sparse():
    i = UdbBaseLinearIndex(['a', 'b', 'c'], sparse=True)
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, None, 3]))


def test_should_not_get_cover_key_on_fully_uncovered_data_as_sparse():
    i = UdbBaseLinearIndex(['a', 'b', 'c'], sparse=True)
    d = {}

    assert i.get_cover_key(d) is None


def test_should_get_cover_key_or_raise_on_full_covered_data():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])
    d = {'a': 1, 'b': 2, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_raise_exception_on_required_field_in_get_cover_key_or_raise():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])

    with pytest.raises(FieldRequiredError):
        i.get_cover_key_or_raise({'a': 1, 'c': 3})


def test_should_get_cover_key_or_raise_on_not_covered_data_with_default_value():
    i = UdbBaseLinearIndex((('a', required), ('b', 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_or_raise_on_not_covered_data_with_default_value_as_callable():
    i = UdbBaseLinearIndex((('a', required), ('b', lambda key, values: 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_or_raise_on_not_covered_data_as_sparse():
    i = UdbBaseLinearIndex(['a', 'b', 'c'], sparse=True)
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, None, 3]))


def test_should_not_get_cover_or_raise_key_on_fully_uncovered_data_as_sparse():
    i = UdbBaseLinearIndex(['a', 'b', 'c'], sparse=True)
    d = {}

    assert i.get_cover_key_or_raise(d) is None