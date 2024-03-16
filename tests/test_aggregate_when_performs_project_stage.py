# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


def test_should_fetch_projected():
    assert list(aggregate([{
        'a': 1,
    }, {
        'b': 2,
    }, {
        'c': 3,
    }], (
        '$project', {'a': 'x', 'b': 'y', 'c': None},
    ))) == [{
        'x': 1,
    }, {
        'y': 2,
    }, {

    }]
