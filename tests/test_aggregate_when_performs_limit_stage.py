# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_limited():
  assert list(aggregate([{
    'a': 1,
  }, {
    'a': 2,
  }, {
    'a': 3,
  }], (
    '$limit', 2,
  ))) == [{
    'a': 1,
  }, {
    'a': 2,
  }]
