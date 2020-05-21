# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_unwound():
  assert list(aggregate([{
    'a': 1, 'x': [1, 2],
  }, {
    'a': 2, 'x': 3,
  }, {
    'a': 3,
  }], (
    '$unwind', 'x',
  ))) == [{
    'a': 1, 'x': 1,
  }, {
    'a': 1, 'x': 2,
  }, {
    'a': 2, 'x': 3,
  }, {
    'a': 3,
  }]
