# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_rebased():
  assert list(aggregate([{
    'a': 1, 'x': {'a': 2, 'b': 2},
  }, {
    'a': 2, 'x': 2,
  }, {
    'a': 3,
  }], (
    '$rebase', 'x',
  ))) == [{
    'a': 2, 'b': 2,
  }, {
    'a': 2, 'x': 2,
  }, {
    'a': 3,
  }]
