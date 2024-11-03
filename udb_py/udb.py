import logging

from .common import Lst
from .index import (
    UdbBtreeBaseIndex,
    UdbBtreeEmbeddedBaseIndex,
    UdbBtreeIndex,
    UdbBtreeEmbeddedIndex,
    UdbBtreeUniqBaseIndex,
    UdbHashBaseIndex,
    UdbHashEmbeddedBaseIndex,
    UdbHashIndex,
    UdbHashEmbeddedIndex,
    UdbHashUniqBaseIndex,
    UdbRtreeIndex,
    UdbTextIndex,
)
from .udb_core import UdbCore


_DELETE_BUFFER_SIZE = 5000
_INDEXES = (
    UdbBtreeBaseIndex,
    UdbBtreeEmbeddedBaseIndex,
    UdbBtreeIndex,
    UdbBtreeEmbeddedIndex,
    UdbBtreeUniqBaseIndex,
    UdbHashBaseIndex,
    UdbHashEmbeddedBaseIndex,
    UdbHashIndex,
    UdbHashEmbeddedIndex,
    UdbHashUniqBaseIndex,
    UdbRtreeIndex,
    UdbTextIndex,
)


class Udb(UdbCore):
    _collection = None
    _copy_on_select = False
    _delete_buffer = None
    _indexes_to_check_for_ins_upd_allowance = None
    _on_delete = None
    _on_insert = None
    _on_update = None
    _revision = 0
    _schema = None
    _storage = None

    @property
    def revision(self):
        return self._revision

    def __init__(self, indexes=None, schema=None, storage=None, indexes_with_custom_seq=None):
        UdbCore.__init__(self, indexes, indexes_with_custom_seq)

        self._collection = {}
        self._delete_buffer = [None] * _DELETE_BUFFER_SIZE
        self._on_delete = []
        self._on_insert = []
        self._on_update = []

        if schema:
            self._schema = schema

        if storage:
            self._storage = storage

            if storage.is_capture_events():
                self._on_delete.append(storage.on_delete)
                self._on_insert.append(storage.on_insert)
                self._on_update.append(storage.on_update)

    def set_copy_on_select(self):
        self._copy_on_select = True

        return self

    def add_on_delete(self, on_delete):
        self._on_delete.append(on_delete)

        return self

    def add_on_insert(self, on_insert):
        self._on_insert.append(on_insert)

        return self

    def add_on_update(self, on_update):
        self._on_update.append(on_update)

        return self

    def load_db(self, mapper=None):
        if not self._storage:
            self._collection = {}
            self._revision = 0

            return False

        logging.debug('db loading')

        data = self._storage.load()

        logging.debug('db loaded')
        
        if not isinstance(data, dict) or 'indexes' not in data or 'data' not in data:
            raise ValueError('invalid db format')

        if mapper:
            self._collection = {int(k): mapper(v) for k, v in data['data'].items()}
        else:
            self._collection = {int(k): v for k, v in data['data'].items()}

        self._revision = data.get('revision', 0)

        if not self._indexes:
            indexes = data['indexes']

            for key, index in indexes.items():
                s_ind = None

                for i in _INDEXES:
                    if index[0] == i.type:
                        s_ind = i

                        break

                if s_ind:
                    indexes[key] = s_ind(**index[1])
                else:
                    raise ValueError('unknown index type: {} on {}'.format(index[1], key))

            self._indexes = indexes

        logging.debug('db indexing')

        for key, record in self._collection.items():
            for index in self._indexes.values():
                index.insert_by_schema(Lst(record) if type(record) == list else record, int(key))

        logging.debug('db indexed')

        self._storage.save_meta(self._indexes, self._revision)

        return True

    def save_db(self, mapper=None):
        if self._storage:
            return self._storage.save(
                self._indexes,
                self._revision,
                {k: mapper(v) for k, v in self._collection.items()} if mapper else self._collection,
            )

        return False

    def delete(self, q=None, limit=None, offset=None):
        delete_count = 0

        while True:
            ind = - 1

            for ind, key in enumerate(self.get_q_cursor(q and cpy_dict(q), limit, offset, get_keys_only=True)):
                self._delete_buffer[ind] = key

                delete_count += 1

                if ind == _DELETE_BUFFER_SIZE - 1:
                    break

            if ind == - 1:
                return delete_count

            for j in range(ind + 1):
                key = self._delete_buffer[j]
                record = self._collection.get(key)

                if self._on_delete:
                    for on_delete in self._on_delete:
                        on_delete(key)

                for index in self._indexes.values():
                    index.delete(index.get_cover_key(record), key)

                self._collection.pop(key)

    def insert(self, values):
        if self._schema:
            for key, schema_entry in self._schema.items():
                if callable(schema_entry):
                    values[key] = schema_entry(key, values)
                elif key not in values:
                    values[key] = schema_entry

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.insert_is_allowed(index.get_cover_key(values))

        values['__rev__'] = self._revision
        self._collection[self._revision] = values

        if self._on_insert:
            for on_insert in self._on_insert:
                on_insert(self._revision, values)

        for index in self._indexes.values():
            index.insert_by_schema(values, self._revision)

        self._revision += 1

        return values

    def update(self, values, q=None, limit=None, offset=None):
        update_count = 0
        self._revision += 1

        for key in self.get_q_cursor(
                q and cpy_dict(q, {'__rev__': {'$lte': self._revision - 1}}),
                limit,
                offset,
                get_keys_only=True
        ):
            before = self._collection.get(key)
            values['__rev__'] = self._revision

            if self._indexes_to_check_for_ins_upd_allowance:
                for index in self._indexes_to_check_for_ins_upd_allowance:
                    index.upsert_is_allowed(index.get_cover_key(before), index.get_cover_key(before, values))

            if self._on_update:
                for on_update in self._on_update:
                    on_update(key, before, values)

            for index in self._indexes.values():
                index.upsert(index.get_cover_key(before), index.get_cover_key(before, values), key)

            self._collection[key].update(values)

            update_count += 1

        return update_count


def cpy_dict(dct, update=None):
    dct = dict(dct)

    return upd_dict(dct, update) if update else dct


def upd_dict(dct, update):
    dct.update(update)

    return dct
