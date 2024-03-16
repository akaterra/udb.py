# -*- coding: utf-8 -*-

import pytest

from udb_py import aggregate, Udb


def test_should_fetch():
    db = Udb()

    db.insert({'x': 1})
    db.insert({'x': 2})

    assert list(aggregate([{
        'a': 1,
    }, {
        'a': 2,
    }, {
        'a': 3,
    }], (
        '$o2o', ('a', 'x', db, 'ref'),
    ))) == [{
        'a': 1, 'ref': {'x': 1, '__rev__': 0},
    }, {
        'a': 2, 'ref': {'x': 2, '__rev__': 1},
    }, {
        'a': 3, 'ref': None,
    }]
