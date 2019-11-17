import pytest

from udbpy.udb import Udb, UdbIndex


class UdbIndexA(UdbIndex):
    q = None

    @classmethod
    def validate_query(cls, q):
        cls.q = q

        return True


class UdbIndexB(UdbIndex):
    q = None

    @classmethod
    def validate_query(cls, q):
        cls.q = q

        return True


@pytest.mark.udb
def test_should_validate_query():
    i = Udb({
        'a': UdbIndexA(['a']),
        'b': UdbIndexB(['b']),
    })

    assert i.validate_query({'a': None}) is True
    assert UdbIndexA.q == {'a': None}
    assert UdbIndexB.q == {'a': None}
