import pytest

from udb_py.common import EMPTY, InvalidScanOperationValueError
from udb_py.udb_index import UdbIndex


@pytest.mark.udb_index
def test_should_be_constructed_with_schema_as_list():
    i = UdbIndex(['a', 'b', 'c'])

    assert i.schema == {'a': EMPTY, 'b': EMPTY, 'c': EMPTY}


@pytest.mark.udb_index
def test_should_be_constructed_with_schema_as_dict():
    i = UdbIndex({'d': EMPTY, 'e': EMPTY, 'f': EMPTY})

    assert i.schema == {'d': EMPTY, 'e': EMPTY, 'f': EMPTY}


@pytest.mark.udb_index
def test_should_validate_query_primitive_value():
    assert UdbIndex.validate_query({'a': None})
    assert UdbIndex.validate_query({'a': False})
    assert UdbIndex.validate_query({'a': True})
    assert UdbIndex.validate_query({'a': 0})
    assert UdbIndex.validate_query({'a': 0.5})
    assert UdbIndex.validate_query({'a': '0'})

    with pytest.raises(InvalidScanOperationValueError):
        UdbIndex.validate_query({'a': []})

    with pytest.raises(InvalidScanOperationValueError):
        UdbIndex.validate_query({'a': UdbIndex})


@pytest.mark.udb_index
def test_should_validate_query_eq_gt_gte_lt_lte_ne_ops():
    for op in ('$eq', '$gt', '$gte', '$lt', '$lte', '$ne'):
        assert UdbIndex.validate_query({'a': {op: None}})
        assert UdbIndex.validate_query({'a': {op: False}})
        assert UdbIndex.validate_query({'a': {op: True}})
        assert UdbIndex.validate_query({'a': {op: 0}})
        assert UdbIndex.validate_query({'a': {op: 0.5}})
        assert UdbIndex.validate_query({'a': {op: '0'}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: []}})
  
        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: {}}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: UdbIndex}})


@pytest.mark.udb_index
def test_should_validate_query_in_nin_ops():
    for op in ('$in', '$nin'):
        assert UdbIndex.validate_query({'a': {op: [None]}})
        assert UdbIndex.validate_query({'a': {op: [False]}})
        assert UdbIndex.validate_query({'a': {op: [True]}})
        assert UdbIndex.validate_query({'a': {op: [0]}})
        assert UdbIndex.validate_query({'a': {op: [0.5]}})
        assert UdbIndex.validate_query({'a': {op: ['0']}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: None}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: False}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: True}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: 0}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: 0.5}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbIndex.validate_query({'a': {op: '0'}})
