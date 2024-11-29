from copy import deepcopy
from typing import Callable, Dict, List, Tuple, Type, Union
from .udb_base_linear_index import SCAN_OP_SEQ
from .udb_btree_index import UdbBtreeIndex
from ..udb_index import UdbIndex


SCAN_OP_CLUSTER = 'cluster'


class UdbClusterIndex(UdbIndex):
    inner_index_cls = UdbBtreeIndex
    is_custom_ops = False
    type = 'cluster'

    def __init__(
            self,
            index_or_btree_index_schema: Union[UdbIndex, dict, List[str]] = None,
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
        self._inner_rids = set()

        if isinstance(inner_index_or_btree_index_schema_or_mapper, UdbIndex):
            self._inner_index_fictive = inner_index_or_btree_index_schema_or_mapper
        elif type(inner_index_or_btree_index_schema_or_mapper) == dict:
            self._inner_index_fictive = self.inner_index_cls(inner_index_or_btree_index_schema_or_mapper)
        elif type(inner_index_or_btree_index_schema_or_mapper) == list:
            self._inner_index_fictive = self.inner_index_cls(inner_index_or_btree_index_schema_or_mapper)
        elif callable(inner_index_or_btree_index_schema_or_mapper):
            self._inner_mapper = inner_index_or_btree_index_schema_or_mapper

        self._clusters: Dict[int, Union[set, Tuple[set, Union[set, UdbIndex]]]] = {}
        self._clusters_key_to_uid = {}
        self._cluster_uid = 0
        self._index = index_or_btree_index_schema\
            if isinstance(index_or_btree_index_schema, UdbIndex)\
            else self.inner_index_cls(index_or_btree_index_schema)
        self.schema_keys = self._index.schema_keys
        self.schema_last_index = self._index.schema_last_index

        if self._inner_index_fictive is not None:
            self.schema_last_index += self._inner_index_fictive.schema_last_index + 1

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

        if not i_op_key_sequence_length:
            return SCAN_OP_SEQ, 0, 0, 0, None, None

        if self._inner_index_fictive is not None:
            t_op = self._inner_index_fictive.get_scan_op(q)
            t_op_key_sequence_length = t_op[1]  # sequence length
            i_op_key_sequence_length += t_op_key_sequence_length / 2

        def fn(k):
            for cluster_id in i_op_fn(k):
                q_cp = deepcopy(q)
                cluster: Union[set, Tuple[set, Union[set, UdbIndex]]] = self._clusters[cluster_id]

                if type(cluster) == set:
                    seq = cluster
                else:
                    (
                        _,
                        _,
                        c_op_key_sequence_length_to_remove,
                        _,
                        c_op_fn,
                        c_op_fn_q_arranger,
                    ) = cluster[1].get_scan_op(q_cp, None, None, collection, indexes_with_custom_ops)
                    key = cluster[1].get_scan_op_cover_key(q_cp, c_op_key_sequence_length_to_remove, c_op_fn_q_arranger)

                    if c_op_fn:
                        seq = c_op_fn(key)
                    else:
                        seq = cluster[0]

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
                cluster = self._clusters[self._cluster_uid] = set(), self._inner_mapper(values, second)
            elif self._inner_index_fictive is not None:
                cluster = self._clusters[self._cluster_uid] = set(), self._inner_index_fictive.clone()
            else:
                cluster = self._clusters[self._cluster_uid] = set()

            self._index.insert(cluster_key, self._cluster_uid)
            self._clusters_key_to_uid[cluster_key] = self._cluster_uid
            self._cluster_uid += 1

        if type(cluster) == set:
            cluster.add(uid)
        else:
            cluster[0].add(uid)
            cluster[1].insert_by_schema(values, uid)

        return True

    def upsert(self, old, new, uid, q=None):
        return self
