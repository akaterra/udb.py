import pytest

from udb_py.common import *
from udb_py.udb import Udb, UdbBtreeIndex, UdbBtreeUniqIndex


@pytest.mark.udb
def test_should_update_all():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    update_count = udb.update({'a': 1})

    assert update_count == 3
    assert list(udb.select()) == [{
        'a': 1, 'b': 1, '__rev__': 4
    }, {
        'a': 1, 'b': 2, '__rev__': 4
    }, {
        'a': 1, 'b': 3, '__rev__': 4
    }]
    assert len(udb.indexes['a']) == 1
    assert len(udb.indexes['ab']) == 3
    assert len(udb.indexes['b']) == 3


@pytest.mark.udb
def test_should_update_by_query():
    udb = Udb({
        'a': UdbBtreeIndex(['a']),
        'ab': UdbBtreeIndex(['a', 'b']),
        'b': UdbBtreeIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    update_count = udb.update({'a': 1}, {'a': 2})

    assert update_count == 1
    assert list(udb.select()) == [{
        'a': 1, 'b': 1, '__rev__': 0
    }, {
        'a': 1, 'b': 2, '__rev__': 4
    }, {
        'a': 3, 'b': 3, '__rev__': 2
    }]
    assert len(udb.indexes['a']) == 2
    assert len(udb.indexes['ab']) == 3
    assert len(udb.indexes['b']) == 3


@pytest.mark.udb
def test_should_raise_conflict_error_on_uniq_index():
    udb = Udb({
        'a': UdbBtreeUniqIndex(['a']),
        'ab': UdbBtreeUniqIndex(['a', 'b']),
        'b': UdbBtreeUniqIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    with pytest.raises(Exception) as excinfo:
        udb.update({'a': 1}, {'a': 2})

    assert isinstance(excinfo.value, ConstraintError) is True
