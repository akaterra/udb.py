import pytest

from udb_py.udb import Udb
from udb_py.udb_index import SCAN_OP_CONST, SCAN_OP_SEQ, SCAN_OP_SORT, SCAN_OP_SUB
from udb_py.index.udb_base_linear_index import SCAN_OP_IN, SCAN_OP_PREFIX, SCAN_OP_PREFIX_IN, SCAN_OP_RANGE
from udb_py.index.udb_base_geo_index import SCAN_OP_INTERSECTION, SCAN_OP_NEAR
from udb_py.index import UdbBtreeIndex


def test_should_plan_const_scan():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': 2}, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_CONST
    assert plan[0][2] == 2
    assert plan[0][3] == 2


def test_should_plan_const_scan_with_dropped_sort_stage():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': 2}, sort='a', get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_CONST
    assert plan[0][2] == 2
    assert plan[0][3] == 2


def test_should_plan_const_scan_in_case_of_like_with_no_pattern_symbols():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': {'$like': '1234567'}}, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_CONST
    assert plan[0][2] == 2
    assert plan[0][3] == 2


def test_should_plan_in_scan():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': {'$in': [2]}}, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_IN
    assert plan[0][2] == 2
    assert plan[0][3] == 2


def test_should_plan_in_scan_with_sort_stage():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': {'$in': [2]}}, sort='-a', get_plan=True)

    assert len(plan) == 2
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_IN
    assert plan[0][2] == 2
    assert plan[0][3] == 2
    assert plan[1][0] is None
    assert plan[1][1] == SCAN_OP_SORT
    assert plan[1][4] == 'a'
    assert plan[1][5] is False


def test_should_plan_in_scan_with_dropped_sort_stage_using_sorted_index():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': {'$in': [2]}}, sort='a', get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_IN
    assert plan[0][2] == 2
    assert plan[0][3] == 2


def test_should_plan_prefix_scan():
    i = Udb({
        # 'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        # 'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'c': 2}, get_plan=True)

    assert len(plan) == 2  # prefix, seq
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_PREFIX
    assert plan[0][2] == 1
    assert plan[0][3] == 1


def test_should_plan_prefix_scan_with_sort_stage():
    i = Udb({
        # 'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        # 'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'c': 2}, sort='c', get_plan=True)

    assert len(plan) == 3  # prefix, seq
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_PREFIX
    assert plan[0][2] == 1
    assert plan[0][3] == 1
    assert plan[2][0] is None
    assert plan[2][1] == SCAN_OP_SORT
    assert plan[2][4] == 'c'
    assert plan[2][5] is True


def test_should_plan_prefix_scan_with_dropped_sort_stage():
    i = Udb({
        # 'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        # 'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'c': 2}, sort='b', get_plan=True)

    assert len(plan) == 2  # prefix, seq
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_PREFIX
    assert plan[0][2] == 1
    assert plan[0][3] == 1


def test_should_plan_prefix_scan_in_case_of_partial_like():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': '1', 'b': {'$like': '345%678'}}, get_plan=True)

    assert len(plan) == 2  # prefix, seq
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_PREFIX
    assert plan[0][2] == 2
    assert plan[0][3] == 1


def test_should_plan_prefix_in_scan():
    i = Udb({
        # 'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        # 'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': {'$in': [1]}, 'c': 2}, get_plan=True)

    assert len(plan) == 2  # prefix in, seq
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_PREFIX_IN
    assert plan[0][2] == 1
    assert plan[0][3] == 1


def test_should_plan_range_scan():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': {'$gte': 2}}, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_RANGE
    assert plan[0][2] == 2
    assert plan[0][3] == 1


def test_should_plan_seq_scan():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'x': 1}, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] is None
    assert plan[0][1] == SCAN_OP_SEQ
    assert plan[0][2] == 0
    assert plan[0][3] == 0


def test_should_plan_additional_seq_scan_on_partial_index_coverage():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1, 'b': 2, 'c': 3}, get_plan=True)

    assert len(plan) == 2
    assert plan[0][1] == SCAN_OP_CONST
    assert plan[1][0] is None
    assert plan[1][1] == SCAN_OP_SEQ
    assert plan[1][2] == 0
    assert plan[1][3] == 0
    assert len(plan[1][4]) == 1
    assert plan[1][4]['c'] == 3


def test_should_plan_sub_scan():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'x': 1}, limit=3, offset=2, get_plan=True)

    assert len(plan) == 2
    assert plan[0][1] == SCAN_OP_SEQ
    assert plan[1][0] is None
    assert plan[1][1] == SCAN_OP_SUB
    assert plan[1][2] == 0
    assert plan[1][3] == 0
    assert plan[1][4] == 3
    assert plan[1][5] == 2


def test_should_not_plan_sub_scan_on_const_not_multivalued_coverage():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1}, limit=3, offset=0, get_plan=True)

    assert len(plan) == 1
    assert plan[0][1] == SCAN_OP_CONST


def test_should_not_plan_any_scan_on_const_not_multivalued_coverage_with_transcending_offset():
    i = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    plan = i.get_q_cursor({'a': 1}, limit=3, offset=3, get_plan=True)

    assert len(plan) == 0
