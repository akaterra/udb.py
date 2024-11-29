import pytest

from udb_py.common import auto_id, current_timestamp, ConstraintError
from udb_py.udb import Udb, UdbBtreeBaseIndex, UdbBtreeUniqBaseIndex


def test_should_raise_conflict_error_on_uniq_index():
    udb = Udb({
        'a': UdbBtreeUniqBaseIndex(['a']),
        'ab': UdbBtreeUniqBaseIndex(['a', 'b']),
        'b': UdbBtreeUniqBaseIndex(['b']),
    })

    a = {'a': 1, 'b': 1}
    b = {'a': 2, 'b': 2}
    c = {'a': 3, 'b': 3}

    udb.insert(a)
    udb.insert(b)
    udb.insert(c)

    with pytest.raises(Exception) as excinfo:
        udb.insert({'a': 2})

    assert isinstance(excinfo.value, ConstraintError) is True
