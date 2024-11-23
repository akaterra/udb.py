from copy import deepcopy
from typing import Callable, List, Type, Union
from .udb_base_linear_index import SCAN_OP_SEQ
from .udb_btree_index import UdbBtreeIndex
from ..udb_index import UdbIndex


SCAN_OP_CLUSTER = 'cluster'


class UdbClusterIndex(UdbIndex):
    is_custom_ops = False
    type = 'cluster'

    def __init__(
            self,
            index_or_btree_index_schema: Union[UdbBtreeIndex, dict, List[str]] = None,
            inner_index_or_btree_index_schema_or_mapper: Union[
                UdbIndex,
                dict,
                List[str],
                Callable[[dict, dict], UdbIndex]
            ] = None,
            name: str = None,
    ):
        UdbIndex.__init__(self, name)

        self._inner_index_fictive = None
        self._inner_mapper = None

        if isinstance(inner_index_or_btree_index_schema_or_mapper, UdbIndex):
            self._inner_index_fictive = inner_index_or_btree_index_schema_or_mapper
        elif type(inner_index_or_btree_index_schema_or_mapper) == dict:
            self._inner_index_fictive = UdbBtreeIndex(inner_index_or_btree_index_schema_or_mapper)
        elif type(inner_index_or_btree_index_schema_or_mapper) == list:
            self._inner_index_fictive = UdbBtreeIndex(inner_index_or_btree_index_schema_or_mapper)
        elif callable(inner_index_or_btree_index_schema_or_mapper):
            self._inner_mapper = inner_index_or_btree_index_schema_or_mapper

        self._clusters = {}
        self._clusters_key_to_uid = {}
        self._cluster_uid = 0
        self._index = UdbBtreeIndex(index_or_btree_index_schema)\
            if index_or_btree_index_schema\
            else index_or_btree_index_schema
        self.schema_keys = self._index.schema_keys
        self.schema_last_index = self._index.schema_last_index

    def get_cover_key(self, record, second=None):
        return self._index.get_cover_key(record, second)

    def get_cover_key_or_raise(self, record, second=None):
        return self._index.get_cover_key_or_raise(record, second)

    def get_indexes_with_custom_ops(self):
        if isinstance(self._inner_index_fictive, UdbIndex):
            return [self._index, self._inner_index_fictive]

        return [self._index]

    def get_meta(self):
        return None

    def get_scan_op(self, q, limit=None, offset=None, collection=None, indexes_with_custom_ops=None):
        (
            i_op_type,
            i_op_key_sequence_length,
            i_op_key_sequence_length_to_remove,
            i_op_priority,
            i_op_fn,
            i_op_fn_q_arranger,
        ) = self._index.get_scan_op(q, None, None, collection, indexes_with_custom_ops)

        if self._inner_index_fictive is not None:
            _, key_len = self._inner_index_fictive.get_cover_key(q)

            if key_len:
                i_op_key_sequence_length += key_len
            elif not any(self._inner_index_fictive.has_key(key) for key in q.keys()):
                return SCAN_OP_SEQ, 0, 0, 0, None, None

        def fn(k):
            for cluster_id in i_op_fn(k):
                q_copy = deepcopy(q)
                cluster = self._clusters[cluster_id]

                if type(cluster) == set:
                    seq = cluster
                else:
                    (
                        c_s_op_type,
                        c_op_key_sequence_length,
                        c_op_key_sequence_length_to_remove,
                        c_op_priority,
                        c_op_fn,
                        c_op_fn_q_arranger,
                    ) = cluster.get_scan_op(q_copy, None, None, collection, indexes_with_custom_ops)
                    key = ''

                    if c_op_key_sequence_length_to_remove:
                        type_format_mappers = cluster.type_format_mappers

                        for i in range(0, c_op_key_sequence_length_to_remove):
                            if i == c_op_key_sequence_length_to_remove - 1 and c_op_fn_q_arranger:
                                pass
                            else:
                                c_key_val = q_copy.pop(cluster.schema_keys[i])
                                key = key + type_format_mappers[type(c_key_val)](c_key_val)

                        if c_op_fn_q_arranger:
                            c_op_fn_q_arranger(q_copy[cluster.schema_keys[c_op_key_sequence_length - 1]])

                            if not q_copy[cluster.schema_keys[c_op_key_sequence_length - 1]]:
                                q_copy.pop(cluster.schema_keys[c_op_key_sequence_length - 1])

                    if c_op_fn:
                        seq = c_op_fn(key)
                    else:
                        seq = cluster.rids()

                if q_copy and indexes_with_custom_ops:
                    for index in indexes_with_custom_ops:
                        seq = index.seq(seq, q_copy, collection)

                for rid in seq:
                    yield rid

        return (
            SCAN_OP_CLUSTER,
            i_op_key_sequence_length,  # cover key length
            i_op_key_sequence_length_to_remove,
            i_op_priority,  # priority
            fn,
            i_op_fn_q_arranger,
        )

    def __len__(self):
        return len(self._index)

    def clear(self):
        self._clusters.clear()
        self._cluster_uid = 0
        self._index.clear()

        return self

    def delete(self, key_or_keys, uid=None, q=None):
        return self

    def insert(self, key_or_keys, uid):
        return self

    def insert_by_schema(self, values, uid):
        if self._index.schema_default_values:
            second = {}

            for key, val in self._index.schema_default_values.items():
                if key not in values:
                    if callable(val):
                        second[key] = val(key, values)
                    else:
                        second[key] = val
        else:
            second = None

        cluster_key = self._index.get_cover_key(values, second)[0]

        if not cluster_key:
            return False

        cluster_uid = self._clusters_key_to_uid.get(cluster_key, None)
        cluster = self._clusters.get(cluster_uid, None) if cluster_uid is not None else None

        if cluster is None:
            if self._inner_mapper is not None:
                cluster = self._clusters[self._cluster_uid] = self._inner_mapper(values, second)
            elif self._inner_index_fictive is not None:
                cluster = self._clusters[self._cluster_uid] = self._inner_index_fictive.clone()
            else:
                cluster = self._clusters[self._cluster_uid] = set()

            self._index.insert(cluster_key, self._cluster_uid)
            self._clusters_key_to_uid[cluster_key] = self._cluster_uid
            self._cluster_uid += 1

        if type(cluster) == set:
            cluster.add(uid)
        else:
            cluster.insert_by_schema(values, uid)

        return True

    def upsert(self, old, new, uid, q=None):
        return self
