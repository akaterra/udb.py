# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_with_no_stages():
  assert list(aggregate([1])) == [1]


def test_should_fetch_with_fn_stages():
  def stage_1(seq, multiplier):
    for val in seq:
      yield val * multiplier

  def stage_2(seq):
    for val in seq:
      yield val + 1

  assert list(aggregate([1, 2, 3], (stage_1, 2), [stage_2])) == [3, 5, 7]
