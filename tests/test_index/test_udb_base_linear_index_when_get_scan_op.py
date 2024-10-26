from udb_py.index.udb_base_linear_index import (
    UdbBaseLinearIndex,
    SCAN_OP_CONST,
    SCAN_OP_IN,
    SCAN_OP_NIN,
    SCAN_OP_PREFIX,
    SCAN_OP_PREFIX_IN,
    SCAN_OP_PREFIX_NIN,
    SCAN_OP_RANGE,
    SCAN_OP_SEQ,
)


class UdbBaseLinearTestIndex(UdbBaseLinearIndex):
    is_prefixed = True
    is_ranged = True

    def search_by_key_eq(self, key):
        return 'search_by_key', key

    def search_by_key_in(self, keys):
        return 'search_by_key_in', keys

    def search_by_key_nin(self, keys):
        return 'search_by_key_nin', keys

    def search_by_key_prefix(self, key):
        return 'search_by_key_prefix', key

    def search_by_key_prefix_in(self, keys):
        return 'search_by_key_prefix_in', keys

    def search_by_key_prefix_nin(self, keys):
        return 'search_by_key_prefix_nin', keys

    def search_by_key_range(self, gte=None, lte=None, gte_excluded=True, lte_excluded=True):
        return 'search_by_key_range', gte, lte, gte_excluded, lte_excluded

    def search_by_key_seq(self, q, source):
        return 'search_by_key_seq', q, source


def test_should_get_const_scan_op():
    i = UdbBaseLinearTestIndex(['a'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None})

    assert op == SCAN_OP_CONST
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 2
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key', '\x04222']
    assert fn_q_arranger is None


def test_should_get_const_scan_op_in_case_of_like_with_no_pattern_symbols():
    i = UdbBaseLinearTestIndex(['a'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$like': '1234567'}})

    assert op == SCAN_OP_CONST
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 2
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key', '\x04222\x041234567']
    assert callable(fn_q_arranger)


def test_should_get_in_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$in': ['000', '111']}})

    assert op == SCAN_OP_IN
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 2
    assert callable(fn)

    params = list(fn('\x04222'))

    assert params[0] == 'search_by_key_in'
    assert list(params[1]) == ['\x04222\x04000', '\x04222\x04111']

    assert callable(fn_q_arranger)


def test_should_get_nin_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$nin': ['000', '111']}})

    assert op == SCAN_OP_NIN
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 2
    assert callable(fn)

    params = list(fn('\x04222'))

    assert params[0] == 'search_by_key_nin'
    assert list(params[1]) == ['\x04222\x04000', '\x04222\x04111']

    assert callable(fn_q_arranger)


def test_should_get_prefix_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': None})

    assert op == SCAN_OP_PREFIX
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 1
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_prefix', '\x04222']
    assert fn_q_arranger is None


def test_should_get_prefix_scan_op_in_case_of_partial_like():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': '1', 'b': '2', 'c': {'$like': '345%678'}})

    assert op == SCAN_OP_PREFIX
    assert prefix_key_len == 3
    assert prefix_key_len_to_remove == 3
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_prefix', '\x04222\x04345']
    assert callable(fn_q_arranger)


def test_should_get_prefix_scan_op_in_case_of_partial_like_2():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': '1', 'b': {'$like': '345%678'}})

    assert op == SCAN_OP_PREFIX
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_prefix', '\x04222\x04345']
    assert callable(fn_q_arranger)


def test_should_get_prefix_scan_op_in_case_of_full_like():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': '1', 'b': '2', 'c': {'$like': '%345678'}})

    assert op == SCAN_OP_PREFIX
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_prefix', '\x04222']
    assert fn_q_arranger is None



def test_should_get_range_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lte': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_range', '\x04222\x04000', '\x04222\x04111', False, False]
    assert callable(fn_q_arranger)


def test_should_get_range_excluding_min_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gt': '000', '$lte': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_range', '\x04222\x04000', '\x04222\x04111', True, False]
    assert callable(fn_q_arranger)


def test_should_get_range_excluding_max_scan_op():
    i = UdbBaseLinearTestIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lt': '111'}})

    assert op == SCAN_OP_RANGE
    assert prefix_key_len == 2
    assert prefix_key_len_to_remove == 2
    assert priority == 1
    assert callable(fn)
    assert list(fn('\x04222')) == ['search_by_key_range', '\x04222\x04000', '\x04222\x04111', False, True]
    assert callable(fn_q_arranger)


def test_should_get_seq_scan_op():
    i = UdbBaseLinearTestIndex(['a'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'x': None})

    assert op == SCAN_OP_SEQ
    assert prefix_key_len == 0
    assert prefix_key_len_to_remove == 0
    assert priority == 0
    assert fn is None
    assert fn_q_arranger is None


def test_should_not_get_range_scan_op_on_not_ranged_index():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': None, 'b': {'$gte': '000', '$lte': '111'}})

    assert op == SCAN_OP_SEQ
    assert prefix_key_len == 0
    assert prefix_key_len_to_remove == 0
    assert priority == 0
    assert fn is None
    assert fn_q_arranger is None
