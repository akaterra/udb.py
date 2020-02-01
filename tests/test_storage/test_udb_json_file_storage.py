import os
import pytest

from udb_py.udb_index import UdbIndex
from udb_py.storage.udb_json_file_storage import UdbJsonFileStorage


def copyfile(inp, out, name, new_name=None):
    with open(inp + '/' + name, 'rb') as r:
        with open(out + '/' + (new_name or name), 'wb') as w:
            chunk = r.read(1024 * 1024 * 16)

            while len(chunk):
                w.write(chunk)

                chunk = r.read(1024 * 1024 * 16)


def test_should_save_db_then_load_db():
    i = {
        'a': UdbIndex(),
        'b': UdbIndex(),
    }

    s = UdbJsonFileStorage('ignore.test')

    s.drop()

    s.save(i, 4, {1: {'a': 1}, 3: {'a': 2}})

    db = s.load()

    assert db == {
        'data': {
            '1': {'a': 1},
            '3': {'a': 2},
        },
        'indexes': {
            'a': [None, {}],
            'b': [None, {}],
        },
        'revision': 4,
    }


def test_should_drop():
    copyfile('tests/test_storage', '.', 'ignore.test.json')

    s = UdbJsonFileStorage('ignore.test')

    s.drop()

    assert os.path.isfile('./ignore.test.json') is False


def test_should_load_db():
    s = UdbJsonFileStorage('ignore.test')

    s.drop()

    copyfile('tests/test_storage', '.', 'ignore.test.json')

    db = s.load()

    assert db == {
        'data': {
            '1': {'a': 1},
            '3': {'a': 2},
        },
        'indexes': {},
        'revision': 4,
    }
