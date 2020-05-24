# -*- coding: utf-8 -*-

import pytest

from udb_py.common import *


@pytest.mark.common
def test_should_format_positive_int_value():
    i = TYPE_FORMAT_MAPPERS[int](1)

    assert i == u'\x03\x01\x00\x00\x00\x00\x00\x00\x00\x01'


@pytest.mark.common
def test_should_compare_formatted_positive_int():
    i = TYPE_FORMAT_MAPPERS[int](1)
    j = TYPE_FORMAT_MAPPERS[int](2)
    k = TYPE_FORMAT_MAPPERS[int](0)

    assert i < j
    assert i > k


@pytest.mark.common
def test_should_format_negative_int_value():
    i = TYPE_FORMAT_MAPPERS[int](-1)

    assert i == u'\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff'


@pytest.mark.common
def test_should_compare_formatted_negative_int():
    i = TYPE_FORMAT_MAPPERS[int](-1)
    j = TYPE_FORMAT_MAPPERS[int](-2)
    k = TYPE_FORMAT_MAPPERS[int](0)

    assert i > j
    assert i < k


@pytest.mark.common
def test_should_format_positive_float_value_with_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](1.9)

    assert i == u'\x03\x01\x00\x00\x00\x00\x00\x00\x00\x01\x03\x01\x0c}q;IÙÿ\x80'


@pytest.mark.common
def test_should_compare_formatted_positive_float_value_with_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](1.9)
    j = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](1.91)
    k = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](1.89)
    l = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[int](1)
    m = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[int](2)

    assert i < j
    assert i > k
    assert i > l
    assert i < m


@pytest.mark.common
def test_should_format_positive_float_value_without_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](1)

    assert i == u'\x03\x01\x00\x00\x00\x00\x00\x00\x00\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00'


@pytest.mark.common
def test_should_format_negative_float_value_with_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](-1.9)

    assert i == u'\x03\x00ÿÿÿÿÿÿÿÿ\x03\x00ó\x82\x8eÄ¶&\x00\x80'


@pytest.mark.common
def test_should_compare_formatted_negative_float_value_with_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](-1.9)
    j = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](-1.91)
    k = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](-1.89)
    l = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[int](-1)
    m = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[int](-2)

    assert i > j
    assert i < k
    assert i < l
    assert i > m


@pytest.mark.common
def test_should_format_negative_float_value_without_decimal():
    i = TYPE_FORMAT_MAPPER_INT_AS_FLOAT[float](-1)

    assert i == u'\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00'


@pytest.mark.common
def test_should_format_true_value():
    i = TYPE_FORMAT_MAPPERS[bool](True)

    assert i == u'\x02\x01'


@pytest.mark.common
def test_should_format_false_value():
    i = TYPE_FORMAT_MAPPERS[bool](False)

    assert i == u'\x02\x00'


@pytest.mark.common
def test_should_format_none_value():
    i = TYPE_FORMAT_MAPPERS[type(None)](None)

    assert i == u'\x01'


@pytest.mark.common
def test_should_format_string_value():
    i = TYPE_FORMAT_MAPPERS[str]('a')

    assert i == u'\x04a'
