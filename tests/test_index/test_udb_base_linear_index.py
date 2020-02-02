import pytest

from udb_py.common import EMPTY, InvalidScanOperationValueError
from udb_py.index.udb_base_linear_index import UdbBaseLinearIndex


@pytest.mark.udb_index
def test_should_be_constructed_with_schema_as_list():
    i = UdbBaseLinearIndex(['a', 'b', 'c'])

    assert i.schema == {'a': EMPTY, 'b': EMPTY, 'c': EMPTY}


@pytest.mark.udb_index
def test_should_be_constructed_with_schema_as_dict():
    i = UdbBaseLinearIndex({'d': EMPTY, 'e': EMPTY, 'f': EMPTY})

    assert i.schema == {'d': EMPTY, 'e': EMPTY, 'f': EMPTY}


@pytest.mark.udb_index
def test_should_validate_query_primitive_value():
    assert UdbBaseLinearIndex.validate_query({'a': None})
    assert UdbBaseLinearIndex.validate_query({'a': False})
    assert UdbBaseLinearIndex.validate_query({'a': True})
    assert UdbBaseLinearIndex.validate_query({'a': 0})
    assert UdbBaseLinearIndex.validate_query({'a': 0.5})
    assert UdbBaseLinearIndex.validate_query({'a': '0'})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseLinearIndex.validate_query({'a': []})

    with pytest.raises(InvalidScanOperationValueError):
        UdbBaseLinearIndex.validate_query({'a': UdbBaseLinearIndex})


@pytest.mark.udb_index
def test_should_validate_query_eq_gt_gte_lt_lte_ne_ops():
    for op in ('$eq', '$gt', '$gte', '$lt', '$lte', '$ne'):
        assert UdbBaseLinearIndex.validate_query({'a': {op: None}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: False}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: True}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: 0}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: 0.5}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: '0'}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: []}})
  
        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: {}}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: UdbBaseLinearIndex}})


@pytest.mark.udb_index
def test_should_validate_query_in_nin_ops():
    for op in ('$in', '$nin'):
        assert UdbBaseLinearIndex.validate_query({'a': {op: [None]}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: [False]}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: [True]}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: [0]}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: [0.5]}})
        assert UdbBaseLinearIndex.validate_query({'a': {op: ['0']}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: None}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: False}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: True}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: 0}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: 0.5}})

        with pytest.raises(InvalidScanOperationValueError):
            UdbBaseLinearIndex.validate_query({'a': {op: '0'}})
