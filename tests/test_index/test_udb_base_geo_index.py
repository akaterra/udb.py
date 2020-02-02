import pytest

from udb_py.common import InvalidScanOperationValueError
from udb_py.index.udb_base_geo_index import UdbBaseGEOIndex


@pytest.mark.udb_index
def test_should_validate_query_intersection_op():
    assert UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'xMin': 0, 'yMin': 0, 'xMax': 0, 'yMax': 0}}})
    assert UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'xMin': 0.5, 'yMin': 0.5, 'xMax': 0.5, 'yMax': 0.5}}})

    invalid_values = [None, False, True, 0, 0.5, '0', [], {}, UdbBaseGEOIndex]

    for invalid_value in invalid_values:
        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$intersection': invalid_value}})

    invalid_values = [None, False, True, '0', [], {}, UdbBaseGEOIndex]

    for invalid_value in invalid_values:
        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$intersection': {
                'xMin': invalid_value,
                'xMax': 0,
                'yMin': 0,
                'yMax': 0,
            }}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$intersection': {
                'xMin': 0,
                'xMax': invalid_value,
                'yMin': 0,
                'yMax': 0,
            }}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$intersection': {
                'xMin': 0,
                'xMax': 0,
                'yMin': invalid_value,
                'yMax': 0,
            }}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$intersection': {
                'xMin': 0,
                'xMax': 0,
                'yMin': 0,
                'yMax': invalid_value,
            }}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'xMin': 0}}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'xMax': 0}}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'yMin': 0}}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$intersection': {'xMax': 0}}})


@pytest.mark.udb_index
def test_should_validate_query_near_op():
    assert UdbBaseGEOIndex.validate_query({'a': {'$near': {'x': 0, 'y': 0, 'maxDistance': 0, 'minDistance': 0}}})
    assert UdbBaseGEOIndex.validate_query({'a': {'$near': {'x': 0.5, 'y': 0.5, 'maxDistance': 0.5, 'minDistance': 0.5}}})

    invalid_values = [None, False, True, 0, 0.5, '0', [], {}, UdbBaseGEOIndex]

    for invalid_value in invalid_values:
        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$near': invalid_value}})

    invalid_values = [None, False, True, '0', [], {}, UdbBaseGEOIndex]

    for invalid_value in invalid_values:
        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$near': {'x': 0, 'y': 0, 'minDistance': invalid_value}}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseGEOIndex.validate_query({'a': {'$near': {'x': 0, 'y': 0, 'maxDistance': invalid_value}}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$near': {'x': 0}}})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseGEOIndex.validate_query({'a': {'$near': {'y': 0}}})
