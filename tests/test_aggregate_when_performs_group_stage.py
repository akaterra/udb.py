# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_grouped_by_keys():
  assert list(aggregate([{
    'a': 1, 'b': 2,
  }, {
    'a': 1, 'b': 2,
  }, {
    'a': 2, 'b': 2,
  }], (
    '$group', ('a', 'b', {}),
  ))) == [{
    'a': 1, 'b': 2,
  }, {
    'a': 2, 'b': 2,
  }]


def test_should_fetch_using_count_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2,
  }, {
    'a': 1, 'b': 2,
  }, {
    'a': 2, 'b': 2,
  }], (
    '$group', ('a', 'b', {'$count': 'count'}),
  ))) == [{
    'a': 1, 'b': 2, 'count': 2,
  }, {
    'a': 2, 'b': 2, 'count': 1,
  }]


def test_should_fetch_using_last_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 1,
  }, {
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 2, 'b': 2, 'c': 3,
  }], (
    '$group', ('a', 'b', {'$last': ('c')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 2, 'b': 2, 'c': 3,
  }]


def test_should_fetch_using_max_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 1,
  }, {
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 2, 'b': 2, 'c': 3,
  }], (
    '$group', ('a', 'b', {'$max': ('c', 'max')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 1, 'max': 2,
  }, {
    'a': 2, 'b': 2, 'c': 3, 'max': 3,
  }]


def test_should_fetch_using_min_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 1,
  }, {
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 2, 'b': 2, 'c': 3,
  }], (
    '$group', ('a', 'b', {'$min': ('c', 'min')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 1, 'min': 1,
  }, {
    'a': 2, 'b': 2, 'c': 3, 'min': 3,
  }]


def test_should_fetch_using_mul_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 1, 'b': 2, 'c': 4,
  }, {
    'a': 2, 'b': 2,
  }], (
    '$group', ('a', 'b', {'$mul': ('c', 'mul')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 2, 'mul': 8,
  }, {
    'a': 2, 'b': 2, 'mul': None,
  }]


def test_should_fetch_using_push_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 1,
  }, {
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 2, 'b': 2,
  }], (
    '$group', ('a', 'b', {'$push': ('c', 'push')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 1, 'push': [1, 2],
  }, {
    'a': 2, 'b': 2, 'push': [],
  }]


def test_should_fetch_using_sum_operation():
  assert list(aggregate([{
    'a': 1, 'b': 2, 'c': 2,
  }, {
    'a': 1, 'b': 2, 'c': 4,
  }, {
    'a': 2, 'b': 2,
  }], (
    '$group', ('a', 'b', {'$sum': ('c', 'sum')}),
  ))) == [{
    'a': 1, 'b': 2, 'c': 2, 'sum': 6,
  }, {
    'a': 2, 'b': 2, 'sum': None,
  }]
