from udb_py.index.udb_base_geo_index import (
    UdbBaseGEOIndex,
    SCAN_OP_INTERSECTION,
    SCAN_OP_NEAR,
)


class UdbBaseGEOTestIndex(UdbBaseGEOIndex):
    def search_by_intersection(self, p_x_min, p_y_min, p_x_max, p_y_max):
        return 'search_by_intersection', p_x_min, p_y_min, p_x_max, p_y_max

    def search_by_near(self, p_x, p_y, min_distance=None, max_distance=None, limit=None, collection=None):
        return 'search_by_near', p_x, p_y, min_distance, max_distance, limit, collection


def test_should_get_intersection_scan_op():
    i = UdbBaseGEOTestIndex('a')

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$intersection': {'minX': 0, 'maxX': 1, 'minY': 2, 'maxY': 3}}})

    assert op == SCAN_OP_INTERSECTION
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 3
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_intersection', 0, 2, 1, 3]
    assert callable(fn_q_arranger)


def test_should_get_near_scan_op():
    i = UdbBaseGEOTestIndex('a')

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$near': {'x': 0, 'y': 1}}})

    assert op == SCAN_OP_NEAR
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 3
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_near', 0, 1, None, None, None, None]
    assert callable(fn_q_arranger)


def test_should_get_near_scan_op_with_min_distance():
    i = UdbBaseGEOTestIndex('a')

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$near': {'x': 0, 'y': 1, 'minDistance': 2}}})

    assert op == SCAN_OP_NEAR
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 3
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_near', 0, 1, 2, None, None, None]
    assert callable(fn_q_arranger)


def test_should_get_near_scan_op_with_max_distance():
    i = UdbBaseGEOTestIndex('a')

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$near': {'x': 0, 'y': 1, 'maxDistance': 2}}})

    assert op == SCAN_OP_NEAR
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 3
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_near', 0, 1, None, 2, None, None]
    assert callable(fn_q_arranger)


def test_should_get_near_scan_op_with_limit():
    i = UdbBaseGEOTestIndex('a')

    op, prefix_key_len, prefix_key_len_to_remove, priority, fn, fn_q_arranger = i.get_scan_op({'a': {'$near': {'x': 0, 'y': 1}}}, limit=1)

    assert op == SCAN_OP_NEAR
    assert prefix_key_len == 1
    assert prefix_key_len_to_remove == 1
    assert priority == 3
    assert callable(fn)
    assert list(fn('\x03222')) == ['search_by_near', 0, 1, None, None, 1, None]
    assert callable(fn_q_arranger)
