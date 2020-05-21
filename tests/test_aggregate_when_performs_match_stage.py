# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_matched():
  assert list(aggregate([{
    'a': 1, 'x': [1, 2],
  }, {
    'a': 2, 'x': 3,
  }, {
    'a': 3,
  }], (
    '$match', {'a': 2},
  ))) == [{
    'a': 2, 'x': 3,
  }]
