# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_from_offset():
    assert list(aggregate([{
        'a': 1,
    }, {
        'a': 2,
    }, {
        'a': 3,
    }], (
        '$offset', 1,
    ))) == [{
        'a': 2,
    }, {
        'a': 3,
    }]
