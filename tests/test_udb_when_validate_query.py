import pytest

from udb_py.udb import Udb
from udb_py.index.udb_base_geo_index import UdbBaseGEOIndex
from udb_py.index.udb_base_linear_index import UdbBaseLinearIndex


class UdbIndexA(UdbBaseGEOIndex):
    q = None

    @classmethod
    def validate_query(cls, q):
        cls.q = q

        return True


class UdbIndexB(UdbBaseLinearIndex):
    q = None

    @classmethod
    def validate_query(cls, q):
        cls.q = q

        return True


@pytest.mark.udb
def test_should_validate_query():
    udb = Udb({
        'a': UdbIndexA('a'),
        'b': UdbIndexB(['b']),
    })

    assert udb.validate_query({'a': None}) is True
    assert UdbIndexA.q == {'a': None}
    assert UdbIndexB.q == {'a': None}
