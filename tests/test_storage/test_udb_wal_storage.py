import os
import pytest

from udb_py.udb_index import UdbIndex
from udb_py.storage.udb_wal_storage import UdbWalStorage


class UdbTestIndex(UdbIndex):
    def get_meta(self):
        return {}


def copyfile(inp, out, name, new_name=None):
    with open(inp + '/' + name, 'rb') as r:
        with open(out + '/' + (new_name or name), 'wb') as w:
            chunk = r.read(1024 * 1024 * 16)

            while len(chunk):
                w.write(chunk)

                chunk = r.read(1024 * 1024 * 16)


def test_should_save_db_then_load_db():
    i = {
        'a': UdbTestIndex(),
        'b': UdbTestIndex(),
    }

    s = UdbWalStorage('ignore.test')

    s.drop()

    s.save_meta(i, 0)

    s.load()

    s.on_insert(1, {'a': 1})
    s.on_insert(2, {'a': 2})
    s.on_insert(3, {'a': 3})
    s.on_delete(2)
    s.on_update(3, {'a': 3}, {'b': 3})

    s.load()

    s.on_update(3, {'a': 3}, {'b': 333})

    db = s.load()

    assert db == {
        'data': {
            1: {'a': 1, '__rev__': 1},
            3: {'a': 3, 'b': 333, '__rev__': 3},
        },
        'indexes': {
            'a': [None, {}],
            'b': [None, {}],
        },
        'revision': 4,
    }


def test_should_drop():
    copyfile('tests/test_storage', '.', 'ignore.test.wal.data')
    copyfile('tests/test_storage', '.', 'ignore.test.wal.data.bak')
    copyfile('tests/test_storage', '.', 'ignore.test.wal.meta')

    s = UdbWalStorage('ignore.test')

    s.drop()

    assert os.path.isfile('./ignore.test.wal.data') is False
    assert os.path.isfile('./ignore.test.wal.data.bak') is False
    assert os.path.isfile('./ignore.test.wal.meta') is False


def test_should_load_db():
    s = UdbWalStorage('ignore.test')

    s.drop()

    copyfile('tests/test_storage', '.', 'ignore.test.wal.data.bak', 'ignore.test.wal.data')
    # copyfile('tests/test_storage', '.', 'ignore.test.wal.data.bak')

    db = s.load()

    assert db == {
        'data': {
            1: {'a': 1, '__rev__': 1},
            3: {'a': 3, 'b': 333, '__rev__': 3},
        },
        'indexes': {},
        'revision': 4,
    }
    assert os.path.isfile('./ignore.test.wal.data.bak') is False


def test_should_load_db_from_backup():
    s = UdbWalStorage('ignore.test')

    s.drop()

    copyfile('tests/test_storage', '.', 'ignore.test.wal.data')
    copyfile('tests/test_storage', '.', 'ignore.test.wal.data.bak')

    db = s.load()

    assert db == {
        'data': {
            1: {'a': 1, '__rev__': 1},
            3: {'a': 3, 'b': 333, '__rev__': 3},
        },
        'indexes': {},
        'revision': 4,
    }
    assert os.path.isfile('./ignore.test.wal.data.bak') is False
