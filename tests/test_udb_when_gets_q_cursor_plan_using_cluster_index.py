import pytest

from udb_py.udb import Udb
from udb_py.udb_index import SCAN_OP_CONST, SCAN_OP_SEQ, SCAN_OP_SORT, SCAN_OP_SUB
from udb_py.index.udb_cluster_index import SCAN_OP_CLUSTER
from udb_py.index import UdbClusterIndex


def test_should_plan_cluster_scan_with_covered_inner_index():
    i = Udb({
        'a': UdbClusterIndex(['a'], ['c']),
        'ab': UdbClusterIndex(['a', 'b'], ['c']),
        'b': UdbClusterIndex(['b'], ['c']),
    })

    q = {'a': 1, 'b': 2, 'c': 3}
    plan = i.get_q_cursor(q, get_plan=True)

    assert len(plan) == 2
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_CLUSTER
    assert plan[0][2] == 2.5
    assert plan[0][3] == 2
    assert q == {'c': 3}


def test_should_plan_cluster_scan_with_non_covered_inner_index():
    i = Udb({
        'a': UdbClusterIndex(['a'], ['c']),
        'ab': UdbClusterIndex(['a', 'b'], ['c']),
        'b': UdbClusterIndex(['b'], ['c']),
    })

    q = {'a': 1, 'b': 2}
    plan = i.get_q_cursor(q, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] == i.indexes['ab']
    assert plan[0][1] == SCAN_OP_CLUSTER
    assert plan[0][2] == 2
    assert plan[0][3] == 2
    assert q == {}


def test_should_not_plan_cluster_scan_with_non_covered_index():
    i = Udb({
        'a': UdbClusterIndex(['a'], ['c']),
        'ab': UdbClusterIndex(['a', 'b'], ['c']),
        'b': UdbClusterIndex(['b'], ['c']),
    })

    q = {'A': 1, 'c': 2}
    plan = i.get_q_cursor(q, get_plan=True)

    assert len(plan) == 1
    assert plan[0][0] is None
    assert plan[0][1] == SCAN_OP_SEQ
    assert plan[0][2] == 0
    assert plan[0][3] == 0
    assert q == {'A': 1, 'c': 2}
