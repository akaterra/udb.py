import pytest

from udb.common import *
from udb.udb_index import *


def test_should_get_cover_key_on_full_covered_data():
    i = UdbIndex(['a', 'b', 'c'])
    d = {'a': 1, 'b': 2, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_not_get_cover_key_on_not_covered_data():
    i = UdbIndex(['a', 'b', 'c'])
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) is None


def test_should_get_cover_key_on_not_covered_data_with_default_value():
    i = UdbIndex((('a', required), ('b', 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_on_not_covered_data_with_default_value_as_callable():
    i = UdbIndex((('a', required), ('b', lambda key, values: 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_on_not_covered_data_as_sparsed():
    i = UdbIndex(['a', 'b', 'c'], sparsed=True)
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key(d) == ''.join(type_formatter_iter([1, None, 3]))


def test_should_not_get_cover_key_on_fully_uncovered_data_as_sparsed():
    i = UdbIndex(['a', 'b', 'c'], sparsed=True)
    d = {}

    assert i.get_cover_key(d) is None


def test_should_get_cover_key_or_raise_on_full_covered_data():
    i = UdbIndex(['a', 'b', 'c'])
    d = {'a': 1, 'b': 2, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_raise_exception_on_required_field_in_get_cover_key_or_raise():
    i = UdbIndex(['a', 'b', 'c'])

    with pytest.raises(FieldRequiredError):
        i.get_cover_key_or_raise({'a': 1, 'c': 3})


def test_should_get_cover_key_or_raise_on_not_covered_data_with_default_value():
    i = UdbIndex((('a', required), ('b', 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_or_raise_on_not_covered_data_with_default_value_as_callable():
    i = UdbIndex((('a', required), ('b', lambda key, values: 2), ('c', required)))
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, 2, 3]))


def test_should_get_cover_key_or_raise_on_not_covered_data_as_sparsed():
    i = UdbIndex(['a', 'b', 'c'], sparsed=True)
    d = {'a': 1, 'c': 3}

    assert i.get_cover_key_or_raise(d) == ''.join(type_formatter_iter([1, None, 3]))


def test_should_not_get_cover_or_raise_key_on_fully_uncovered_data_as_sparsed():
    i = UdbIndex(['a', 'b', 'c'], sparsed=True)
    d = {}

    assert i.get_cover_key_or_raise(d) is None
