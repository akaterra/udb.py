from .index import (
    UdbBaseGEOIndex,
    UdbBaseLinearIndex,
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
from .udb_index import (
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


class UdbCore(object):
    _collection = None
    _copy_on_select = False
    _custom_check_condition = [UdbBaseLinearIndex.check_condition, UdbBaseGEOIndex.check_condition]
    _custom_seq = [UdbBaseLinearIndex.seq, UdbBaseGEOIndex.seq]
    _custom_validate_query = [UdbBaseLinearIndex.validate_query, UdbBaseGEOIndex.validate_query]
    _indexes = {}

    @property
    def collection(self):
        return self._collection

    @property
    def indexes(self):
        return self._indexes

    def __init__(self, indexes=None, indexes_with_custom_seq=None):
        self._collection = {}

        if indexes:
            self._indexes = indexes
            self._indexes_to_check_for_ins_upd_allowance = [index for index in indexes.values() if index.is_uniq]

            for key, ind in indexes.items():
                if ind.seq not in self._custom_validate_query:
                    self._custom_validate_query.append(ind.validate_query)

                ind.name = key

        if indexes_with_custom_seq:
            self._custom_check_condition = [
                index_with_custom_seq.check_condition for index_with_custom_seq in indexes_with_custom_seq
            ]
            self._custom_seq = [
                index_with_custom_seq.seq for index_with_custom_seq in indexes_with_custom_seq
            ]

    def __len__(self):
        return 0

    def get_index(self, key):
        return self._indexes[key]

    def select(self, q=None, limit=None, offset=None, sort=None, use_indexes=None, get_plan=False):
        return self.get_q_cursor(q, limit, offset, sort, use_indexes=use_indexes, get_plan=get_plan)

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
        sort_direction = None if sort is None or sort_is_fn else sort[0] != '-'

        plan = [] if get_plan else None

        if q:
            for custom_seq in \
                    map(lambda ind: self._indexes[ind], use_indexes) \
                    if use_indexes \
                    else self._indexes.values():
                if custom_seq.schema_last_index < s_op_key_sequence_length:
                    continue

                (
                    c_s_op_type,
                    c_op_key_sequence_length,
                    c_op_priority,
                    c_op_fn,
                    c_op_fn_q_arranger,
                ) = custom_seq.get_scan_op(
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
            if s_index.is_sorted_asc and sort_direction:
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
            limit = 999999999

        if offset is None:
            offset = 0

        if offset:
            for k in source:
                offset -= 1

                if not offset:
                    break

        if limit:
            for k in source:
                yield k

                limit -= 1

                if not limit:
                    break


def cpy_dict(dct, update=None):
    dct = dict(dct)

    return upd_dict(dct, update) if update else dct


def upd_dict(dct, update):
    dct.update(update)

    return dct
