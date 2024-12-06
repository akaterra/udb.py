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
        self._clusters_key_to_rid = {}
        self._cluster_rid = 0
        self._index = index_or_btree_index_schema\
            if isinstance(index_or_btree_index_schema, UdbIndex)\
            else self.inner_index_cls(index_or_btree_index_schema)
        self.schema_keys = self._index.schema_keys
        self.schema_last_index = self._index.schema_last_index

        if self._inner_index_fictive is not None:
            self.schema_last_index += self._inner_index_fictive.schema_last_index + 1

    def __len__(self):
        length = 0

        for cluster in self._clusters.values():
            length += len(cluster) if type(cluster) == set else len(cluster[1])

        return length

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

    def clear(self):
        self._clusters.clear()
        self._cluster_rid = 0
        self._index.clear()

        return self

    def delete(self, key_or_keys, rid=None, q=None):
        return self

    def insert(self, key_or_keys, rid):
        return self

    def insert_by_schema(self, values, rid):
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

        cluster_rid = self._clusters_key_to_rid.get(cluster_key, None)
        cluster = self._clusters.get(cluster_rid, None) if cluster_rid is not None else None

        if cluster is None:
            if self._inner_mapper is not None:
                cluster = self._clusters[self._cluster_rid] = set(), self._inner_mapper(values, second)
            elif self._inner_index_fictive is not None:
                cluster = self._clusters[self._cluster_rid] = set(), self._inner_index_fictive.clone()
            else:
                cluster = self._clusters[self._cluster_rid] = set()

            self._index.insert(cluster_key, self._cluster_rid)
            self._clusters_key_to_rid[cluster_key] = self._cluster_rid
            self._cluster_rid += 1

        if type(cluster) == set:
            cluster.add(rid)
        else:
            cluster[0].add(rid)
            cluster[1].insert_by_schema(values, rid)

        return True

    def upsert(self, old, new, rid, q=None):
        if old == new:
            return True

        old_cluster = self._clusters.get(self._clusters_key_to_rid.get(old, None), None)

        if not q:
            if old_cluster is not None:
                old_cluster_set = old_cluster if type(old_cluster) == set else old_cluster[0]
                old_cluster_ind = None if type(old_cluster) == set else old_cluster[1]

                new_cluster = self._clusters.get(self._clusters_key_to_rid.get(new, None), None)

                if new_cluster is not None:
                    new_cluster_set = new_cluster if type(new_cluster) == set else new_cluster[0]
                    new_cluster_ind = None if type(new_cluster) == set else new_cluster[1]

                    new_cluster_set.update(old_cluster_set)

                    if new_cluster_ind is not None:
                        for old_key, old_rid in old_cluster_set if old_cluster_ind is None else old_cluster_ind.keys_and_rids():
                            new_cluster_ind.insert(old_key, old_rid)
                else:
                    self._clusters_key_to_rid[new] = self._clusters_key_to_rid[old]

                del self._clusters[self._clusters_key_to_rid[old]]
                del self._clusters_key_to_rid[old]
                self._index.upsert(old, new, self._clusters_key_to_rid[new])
            else:
                return False

            # old_cluster_key = self._clusters_key_to_rid.get(old, None)
            # old_cluster = self._clusters.get(old_cluster_key, None) if old_cluster_key is not None else None
            #
            # if old_cluster is not None:
            #     if type(old_cluster) == set:
            #         old_cluster.remove(rid)
            #     else:
            #         old_cluster[0].remove(rid)
            #
            # new_cluster_key = self._clusters_key_to_rid.get(new, None)
            # new_cluster = self._clusters.get(new_cluster_key, None) if new_cluster_key is not None else None
            #
            # if new_cluster is None:
            #     if self._inner_mapper is not None:
            #         new_cluster = self._clusters[self._cluster_rid] = set(), self._inner_mapper({})
            #     elif self._inner_index_fictive is not None:
            #         new_cluster = self._clusters[self._cluster_rid] = set(), self._inner_index_fictive.clone()
            #     else:
            #         new_cluster = self._clusters[self._cluster_rid] = set()
            #
            #     self._index.insert(new_cluster_key, self._cluster_rid)
            #     self._clusters_key_to_rid[new_cluster_key] = self._cluster_rid
            #     self._cluster_rid += 1
            #
            # if new_cluster is not None:
            #     if type(new_cluster) == set:
            #         new_cluster.add(rid)
            #     else:
            #         new_cluster[0].add(rid)

        return self
