from .aggregate import aggregate, register_aggregation_pipe, SKIP
from .common import cpy_dict, sort_key_iter
from .index import (
    UdbBaseGEOIndex,
    UdbBaseLinearIndex,
    UdbBaseTextIndex,
)
from .udb_index import (
    SCAN_OP_CONST,
    SCAN_OP_SEQ,
    SCAN_OP_SORT,
    SCAN_OP_SUB,
)


class UdbCore(object):
    _collection = None
    _copy_on_select = False
    _indexes_with_custom_ops = {UdbBaseLinearIndex, UdbBaseGEOIndex}
    _indexes = {}

    @property
    def collection(self):
        return self._collection

    @property
    def indexes(self):
        return self._indexes

    @classmethod
    def register_aggregation_pipe(cls, pipe, fn):
        register_aggregation_pipe(pipe, fn)
        
        return cls

    @classmethod
    def register_index(cls, index_class):
        cls._indexes_with_custom_ops.add(index_class)
        
        return cls

    def __init__(self, indexes=None, indexes_with_custom_ops=None):
        self._collection = {}

        if indexes_with_custom_ops:
            self._indexes_with_custom_ops = set(indexes_with_custom_ops)

        if indexes:
            self._indexes = indexes
            self._indexes_to_check_for_ins_upd_allowance = [index for index in indexes.values() if index.is_uniq]

            for key, ind in indexes.items():
                if type(ind) not in self._indexes_with_custom_ops:
                    self._indexes_with_custom_ops.add(type(ind))

                ind.name = key

    def __len__(self):
        return 0

    def get_index(self, key):
        return self._indexes[key]

    def set_copy_on_select(self):
        self._copy_on_select = True

        return self

    def aggregate(self, *pipes, q=None, limit=None, offset=None, sort=None, use_indexes=None):
        return aggregate(self.get_q_cursor(q, limit, offset, sort, use_indexes=use_indexes), *pipes)

    def select(self, q=None, limit=None, offset=None, sort=None, use_indexes=None, get_plan=False):
        return self.get_q_cursor(q, limit, offset, sort, use_indexes=use_indexes, get_plan=get_plan)

    def select_one(self, q=None, offset=None, use_indexes=None):
        for record in self.get_q_cursor(q, 1, offset, None, use_indexes=use_indexes):
            return record
        
        return None

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
        s_op_key_sequence_length_to_remove = 0
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
                    c_op_key_sequence_length_to_remove,
                    c_op_priority,
                    c_op_fn,
                    c_op_fn_q_arranger,
                ) = custom_seq.get_scan_op(
                    q,
                    limit,
                    offset,
                    self._collection,
                )

                if s_op_key_sequence_length < c_op_key_sequence_length \
                        or s_op_key_sequence_length_to_remove < c_op_key_sequence_length_to_remove:
                    s_index = custom_seq
                    s_op_type = c_s_op_type
                    s_op_key_sequence_length = c_op_key_sequence_length
                    s_op_key_sequence_length_to_remove = c_op_key_sequence_length_to_remove
                    s_op_priority = c_op_priority
                    s_op_fn = c_op_fn
                    s_op_fn_q_arranger = c_op_fn_q_arranger
                elif s_op_priority < c_op_priority:
                    s_index = custom_seq
                    s_op_type = c_s_op_type
                    s_op_key_sequence_length = c_op_key_sequence_length
                    s_op_key_sequence_length_to_remove = c_op_key_sequence_length_to_remove
                    s_op_priority = c_op_priority
                    s_op_fn = c_op_fn
                    s_op_fn_q_arranger = c_op_fn_q_arranger

        if s_index is not None:
            if s_op_type == SCAN_OP_CONST:
                if not s_index.is_multivalued:
                    if offset:
                        return []

                    limit = sort = None

            key = ''
            type_format_mappers = s_index.type_format_mappers

            for i in range(0, s_op_key_sequence_length_to_remove):
                if i == s_op_key_sequence_length_to_remove - 1 and s_op_fn_q_arranger:
                    pass
                else:
                    c_key_val = q.pop(s_index.schema_keys[i])
                    key = key + type_format_mappers[type(c_key_val)](c_key_val)

            if s_op_fn_q_arranger:
                s_op_fn_q_arranger(q[s_index.schema_keys[s_op_key_sequence_length - 1]])

                if not q[s_index.schema_keys[s_op_key_sequence_length - 1]]:
                    q.pop(s_index.schema_keys[s_op_key_sequence_length - 1])

            if get_plan:
                plan.append((s_index, s_op_type, s_op_key_sequence_length, s_op_priority))
            else:
                seq = s_op_fn(key)

            if not sort_is_fn and s_index.is_sorted_asc and sort_direction and s_index.has_key(sort):
                sort = None
        else:
            if get_keys_only or q:
                seq = self._collection.keys()
            else:
                seq = self._collection.values()

        if get_plan:
            if q:
                plan.append((None, SCAN_OP_SEQ, 0, 0, q))

            if limit or offset:
                plan.append((None, SCAN_OP_SUB, 0, 0, limit, offset))

            if sort:
                plan.append((None, SCAN_OP_SORT, 0, 0, sort if sort_direction else sort[1:], sort_direction))

            return plan

        if q and self._indexes_with_custom_ops:
            for index in self._indexes_with_custom_ops:
                seq = index.seq(seq, q, self._collection)

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
                    seq = sort_key_iter(sort, seq)
                else:
                    seq = sort_key_iter(sort[1:], seq, reverse=True)

        return seq

    def validate_query(self, q):
        if self._indexes_with_custom_ops:
            for index in self._indexes_with_custom_ops:
                index.validate_query(q)

        return True

    def _get_aggregation_cursor(self, source, aggregations):
        for record in source:
            for op, args in aggregations:
                if op == 'o2o':
                    record[args[3]] = args[0].select_one({args[1]: record[args[2]]})
                elif op == 'o2m':
                    record[args[3]] = list(args[0].select({args[1]: record[args[2]]}))

            yield record                

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


def _match_aggregation_pipe(seq, q, with_facet=False):
    index_context = [(index, index.create_condition_context(q)) for index in UdbCore._indexes_with_custom_ops]

    for record in seq:
        if record == SKIP:
            yield record
            continue

        passed = True

        for index, context in index_context:
            passed = index.check_condition(record, q, context)

            if not passed:
                if with_facet:
                    yield SKIP

                break

        if passed:
            yield record


UdbCore.register_aggregation_pipe('$match', _match_aggregation_pipe)
