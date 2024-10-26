# -*- coding: utf-8 -*-

import pytest

from udb_py.aggregate import aggregate


# def test_should_apply_multi_facet():
#     result = list(aggregate([{
#         'a': 1, 'b': 2,
#     }, {
#         'a': 1, 'b': 3,
#     }, {
#         'a': 2, 'b': 4,
#     }, {
#         'a': 2, 'b': 4,
#     }, {
#         'a': 2, 'b': 4,
#     }], (
#         '$facet', {
#             'a': [
#                 ('$match', {'a': 1}),
#                 ('$facet', {
#                     'a': [('$match', {'b': 2})],
#                     'b': [('$match', {'b': 3})],
#                 }),
#             ],
#             'b': [('$match', {'a': 2}), ('$limit', 1)],
#         },
#     )))
#
#     assert result == [{
#         'a': [{'a': 1, 'b': 2}, {'a': 1, 'b': 3}],
#         'b': [{'a': 2, 'b': 4}],
#     }]
