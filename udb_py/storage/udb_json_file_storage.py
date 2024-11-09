import json
import os

from ..udb_storage import UdbStorage


BUILT_IN_TYPES = {
    type(None): True,
    bool: True,
    dict: True,
    float: True,
    int: True,
    list: True,
    str: True,
    tuple: True,
}


class UdbJsonFileStorage(UdbStorage):
    _name = None

    def __init__(self, name):
        self._name = name

    def is_available(self):
        return os.path.isfile(self._name + '.json')

    def drop(self):
        if os.path.isfile(self._name + '.json'):
            os.remove(self._name + '.json')

        return self

    def load(self):
        if self.is_available():
            with open(self._name + '.json', 'r') as file_r_desc:
                data = json.load(file_r_desc)

            return data

        return {'indexes': {}, 'revision': 0, 'data': {}}

    def save(self, indexes, revision, data):
        with open(self._name + '.json', 'w+') as file_w_desc:
            json.dump({
                'indexes': {k: [v.type, v.get_meta()] for k, v in indexes.items()},
                'revision': revision,
                'data': data,
            }, file_w_desc, indent=2)

        return True

    def save_meta(self, indexes, revision):
        return self


def _encode(value):
    if type(value) in BUILT_IN_TYPES:
        return value

    return value.__getstate__()
