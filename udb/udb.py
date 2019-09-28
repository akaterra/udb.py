import logging

from .common import Lst, auto_id, current_timestamp, fn, optional, required
from .index import (
    UdbBtreeIndex,
    UdbBtreeEmbeddedIndex,
    UdbBtreeMultivaluedIndex,
    UdbBtreeMultivaluedEmbeddedIndex,
    UdbBtreeUniqIndex,
    UdbHashIndex,
    UdbHashEmbeddedIndex,
    UdbHashMultivaluedIndex,
    UdbHashMultivaluedEmbeddedIndex,
    UdbHashUniqIndex,
    UdbRtreeIndex,
)
from .storage import (
    UdbJsonFileStorage,
    UdbWalStorage,
)
from .udb_index import (
    UdbIndex,
    EMPTY,
    SCAN_OP_CONST,
    SCAN_OP_SEQ,
    SCAN_OP_SORT,
    SCAN_OP_SUB,
)

_DELETE_BUFFER_SIZE = 5000
_INDEXES = (
    UdbBtreeIndex,
    UdbBtreeEmbeddedIndex,
    UdbBtreeMultivaluedIndex,
    UdbBtreeMultivaluedEmbeddedIndex,
    UdbBtreeUniqIndex,
    UdbHashIndex,
    UdbHashEmbeddedIndex,
    UdbHashMultivaluedIndex,
    UdbHashMultivaluedEmbeddedIndex,
    UdbHashUniqIndex,
    UdbRtreeIndex,
)


class Udb(object):
    _collection = None
    _copy_on_select = False
    _custom_seq = [UdbIndex.seq, UdbRtreeIndex.seq]
    _custom_validate_query = [UdbIndex.validate_query, UdbRtreeIndex.validate_query]
    _delete_buffer = None
    _indexes = {}
    _indexes_to_check_for_ins_upd_allowance = None
    _on_delete = None
    _on_insert = None
    _on_update = None
    _revision = 0
    _schema = None
    _storage = None

    @property
    def collection(self):
        return self._collection

    @property
    def indexes(self):
        return self._indexes

    @property
    def revision(self):
        return self._revision

    def __init__(self, indexes=None, schema=None, storage=None, indexes_with_custom_seq=None):
        self._collection = {}
        self._delete_buffer = [None] * _DELETE_BUFFER_SIZE

        if indexes:
            self._indexes = indexes
            self._indexes_to_check_for_ins_upd_allowance = [index for index in indexes.values() if index.is_uniq]

            for key, ind in indexes.items():
                if ind.seq not in self._custom_validate_query:
                    self._custom_validate_query.append(ind.validate_query)

                ind.name = key

        if indexes_with_custom_seq:
            self._custom_seq = [index_with_custom_seq.seq for index_with_custom_seq in indexes_with_custom_seq]

        if schema:
            self._schema = schema

        if storage:
            self._storage = storage

            if storage.is_capture_events():
                self._on_delete = storage.on_delete
                self._on_insert = storage.on_insert
                self._on_update = storage.on_update

    def __len__(self):
        return 0

    def get_index(self, key):
        return self._indexes[key]

    def set_copy_on_select(self):
        self._copy_on_select = True

        return self

    def load_db(self, mapper=None):
        if self._storage:
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

                for k, index in indexes.items():
                    s_ind = None

                    for i in _INDEXES:
                        if index[1] == i.type:
                            s_ind = i

                            break

                    if s_ind:
                        indexes[k] = s_ind(index[0], k)
                    else:
                        raise ValueError('unknown index type: {} on {}'.format(index[1], k))

                self._indexes = indexes

            logging.debug('db indexing')

            for k, v in self._collection.items():
                for index in self._indexes.values():
                    index.insert_by_schema(Lst(v) if type(v) == list else v, int(k))

            logging.debug('db indexed')

            self._storage.save_meta(self._indexes, self._revision)

            return True
        else:
            self._collection = {}
            self._revision = 0

        return False

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
            i = - 1

            for i, k in enumerate(self.get_q_cursor(q and cpy_dict(q), limit, offset, get_keys_only=True)):
                self._delete_buffer[i] = k

                delete_count += 1

                if i == _DELETE_BUFFER_SIZE - 1:
                    break

            if i == - 1:
                return delete_count

            for j in range(i + 1):
                k = self._delete_buffer[j]
                record = self._collection.get(k)

                if self._on_delete:
                    self._on_delete(k)

                for index in self._indexes.values():
                    index.delete(index.get_cover_key(record), k)

                self._collection.pop(k)

    def insert(self, values):
        if self._schema:
            for key, val in self._schema.items():
                if callable(val):
                    values[key] = val(key, values)
                elif key not in values:
                    values[key] = val

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.insert_is_allowed(index.get_cover_key(values))

        if self._on_insert:
            self._on_insert(self._revision, values)

        for index in self._indexes.values():
            index.insert_by_schema(values, self._revision)

        self._collection[self._revision] = cpy_dict(values, {'__rev__': self._revision})
        self._revision += 1

        return values

    def select(self, q=None, limit=None, offset=None, sort=None, use_indexes=None, get_plan=False):
        return self.get_q_cursor(q, limit, offset, sort, use_indexes=use_indexes, get_plan=get_plan)

    def update(self, values, q=None, limit=None, offset=None):
        update_count = 0
        self._revision += 1

        for i, k in enumerate(self.get_q_cursor(
            q and cpy_dict(q, {'__rev__': {'$lte': self._revision - 1}}),
            limit,
            offset,
            get_keys_only=True
        )):
            before = self._collection.get(k)
            values['__rev__'] = self._revision

            if self._indexes_to_check_for_ins_upd_allowance:
                for index in self._indexes_to_check_for_ins_upd_allowance:
                    index.upsert_is_allowed(index.get_cover_key(before), index.get_cover_key(before, values))

            if self._on_update:
                self._on_update(k, before, values)

            for index in self._indexes.values():
                index.upsert(index.get_cover_key(before), index.get_cover_key(before, values), k)

            self._collection[k].update(values)

            update_count += 1

        return update_count

    def get_q_cursor(
        self,
        q=None,
        limit=None,
        offset=None,
        sort=None,
        get_plan=False,
        get_keys_only=False,
        use_indexes=None
    ):
        """
        :param q:
        :param limit:
        :param offset:
        :param sort:
        :param get_plan:
        :param get_keys_only:
        :param use_indexes:

        :return:
        """
        s_index = None
        s_op_type = None
        s_op_key_sequence_length = 0
        s_op_priority = 0
        s_op_fn = None
        s_op_fn_q_arranger = None

        sort_is_fn = sort and callable(sort)
        sort_direction = None if sort is None or sort_is_fn else False if sort[0] == '-' else True

        plan = [] if get_plan else None

        if q:
            for custom_seq in map(lambda ind: self._indexes[ind], use_indexes) if use_indexes else self._indexes.values():
                if custom_seq.schema_last_index < s_op_key_sequence_length:
                    continue

                c_s_op_type, c_op_key_sequence_length, c_op_priority, c_op_fn, c_op_fn_q_arranger = custom_seq.get_scan_op(
                    q,
                    limit,
                    offset,
                    self._collection
                )

                if s_op_key_sequence_length < c_op_key_sequence_length:
                    s_index = custom_seq
                    s_op_type = c_s_op_type
                    s_op_key_sequence_length = c_op_key_sequence_length
                    s_op_priority = c_op_priority
                    s_op_fn = c_op_fn
                    s_op_fn_q_arranger = c_op_fn_q_arranger
                elif s_op_priority < c_op_priority:
                    s_index = custom_seq
                    s_op_type = c_s_op_type
                    s_op_key_sequence_length = c_op_key_sequence_length
                    s_op_priority = c_op_priority
                    s_op_fn = c_op_fn
                    s_op_fn_q_arranger = c_op_fn_q_arranger

        if s_index is not None:
            if s_index.is_sorted and sort_direction:
                sort = None

            if s_op_type == SCAN_OP_CONST:
                if not s_index.is_multivalued:
                    if offset:
                        return []

                    limit = sort = None

            key = ''
            type_format_mappers = s_index.type_format_mappers

            for i in range(0, s_op_key_sequence_length):
                if i == s_op_key_sequence_length - 1 and s_op_fn_q_arranger:
                    pass
                else:
                    c_key_val = q.pop(s_index.schema_keys[i])
                    key = key + type_format_mappers[type(c_key_val)](c_key_val)

            if s_op_fn_q_arranger:
                s_op_fn_q_arranger(q[s_index.schema_keys[s_op_key_sequence_length - 1]])

                if not len(q[s_index.schema_keys[s_op_key_sequence_length - 1]]):
                    q.pop(s_index.schema_keys[s_op_key_sequence_length - 1])

            if get_plan:
                plan.append((s_index, s_op_type, s_op_key_sequence_length, s_op_priority))
            else:
                seq = s_op_fn(key)
        else:
            if get_keys_only or q:
                seq = self._collection.keys()
            else:
                seq = self._collection.values()

        if get_plan:
            if len(q):
                plan.append((None, SCAN_OP_SEQ, 0, 0, q))

            if limit or offset:
                plan.append((None, SCAN_OP_SUB, 0, 0, limit, offset))

            if sort:
                plan.append((None, SCAN_OP_SORT, 0, 0, sort if sort_direction else sort[1:], sort_direction))

            return plan

        if q and self._custom_seq:
            for custom_seq in self._custom_seq:
                seq = custom_seq(seq, q, self._collection)

        if limit or offset:
            seq = self._get_subset_cursor(seq, limit, offset)

        if not get_keys_only:
            if s_index is not None or q:
                seq = self._get_collection_fetch_cursor(seq)

        if sort:
            if sort_is_fn:
                seq = sort(seq)
            else:
                if sort_direction:
                    seq = iter(sorted(seq, key=lambda record: record.get(sort)))
                else:
                    seq = iter(sorted(seq, key=lambda record: record.get(sort[1:]), reverse=True))

        return seq

    def validate_query(self, q):
        if self._custom_validate_query:
            for custom_validate_query in self._custom_validate_query:
                custom_validate_query(q)

        return True

    def _get_collection_fetch_cursor(self, source):
        if self._copy_on_select:
            for k in source:
                yield cpy_dict(self._collection.get(k))
        else:
            for k in source:
                yield self._collection.get(k)

    def _get_subset_cursor(self, source, limit=None, offset=None):
        if limit is None:
            limit = 9999999999

        if offset is None:
            offset = 0

        while offset:
            next(source)

            offset -= 1

        while limit:
            yield next(source)

            limit -= 1


def cpy_dict(d, update=None):
    d = dict(d)

    return upd_dict(d, update) if update else d


def upd_dict(d, update):
    d.update(update)

    return d
