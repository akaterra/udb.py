from udb_py.udb_index import *


class UdbIndexTest(UdbIndex):
    is_prefixed = True
    is_ranged = True

    def search_by_key(self, key):
        return 'search_by_key', key

    def search_by_key_in(self, keys):
        return 'search_by_key_in', keys

    def search_by_key_prefix(self, key):
        return 'search_by_key_prefix', key

    def search_by_key_prefix_in(self, keys):
        return 'search_by_key_prefix_in', keys

    def search_by_key_range(self, gte=None, lte=None, gte_excluded=True, lte_excluded=True):
        return 'search_by_key_range', gte, lte, gte_excluded, lte_excluded

    def search_by_key_seq(self, q, source):
        return 'search_by_key_seq', q, source


def test_should_get_const_scan_op():
    i = UdbIndexTest(['a'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None})

    assert op == SCAN_OP_CONST
    assert prefix_key_len == 1
    assert priority == 2
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_key', '\x03222']
    assert fn_q_arranger is None


def test_should_get_in_scan_op():
    i = UdbIndexTest(['a', 'b'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$in': ['000', '111']}})

    assert op == SCAN_OP_IN
    assert prefix_key_len == 2
    assert priority == 2
    assert callable(fn)

    params = list(fn('\x03222'))

    assert params[0] == 'search_by_key_in'
    assert list(params[1]) == ['\x03222\x03000', '\x03222\x03111']

    assert callable(fn_q_arranger)


def test_should_get_prefix_scan_op():
    i = UdbIndexTest(['a', 'b', 'c'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': None})

    assert op == SCAN_OP_PREFIX
    assert prefix_key_len == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_key_prefix', '\x03222']
    assert fn_q_arranger is None


def test_should_get_range_scan_op():
    i = UdbIndexTest(['a', 'b', 'c'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lte': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_key_range', '\x03222\x03000', '\x03222\x03111', False, False]
    assert callable(fn_q_arranger)


def test_should_get_range_excluding_min_scan_op():
    i = UdbIndexTest(['a', 'b', 'c'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gt': '000', '$lte': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_key_range', '\x03222\x03000', '\x03222\x03111', True, False]
    assert callable(fn_q_arranger)


def test_should_get_range_excluding_max_scan_op():
    i = UdbIndexTest(['a', 'b', 'c'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lt': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_key_range', '\x03222\x03000', '\x03222\x03111', False, True]
    assert callable(fn_q_arranger)


def test_should_get_seq_scan_op():
    i = UdbIndexTest(['a'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'x': None})

    assert op == SCAN_OP_SEQ
    assert prefix_key_len == 0
    assert priority == 0
    assert fn is None
    assert fn_q_arranger is None


def test_should_not_get_range_scan_op_on_not_ranged_index():
    i = UdbIndex(['a', 'b', 'c'])

    op, prefix_key_len, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lte': '111'}})

    assert op == SCAN_OP_SEQ
    assert prefix_key_len == 0
    assert priority == 0
    assert fn is None
    assert fn_q_arranger is None
