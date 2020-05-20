from .common import cpy_dict
from .udb_core import UdbCore


class UdbView(UdbCore):
    _query = None
    _udb = None

    def __init__(self, udb, query, indexes=None):
        if indexes is True:
            indexes = cpy_dict(udb.indexes)

        UdbCore.__init__(self, indexes)

        self._collection = udb.collection
        self._query = query
        self._udb = udb

        if indexes:
            udb\
                .add_on_delete(self._on_delete)\
                .add_on_insert(self._on_insert)\
                .add_on_update(self._on_update)

            for rid in udb.get_q_cursor(cpy_dict(query), get_keys_only=True):
                self._on_insert(rid, self._collection[rid])

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
        if q:
            for key, val in q.items():
                if key in self._query:
                    if type(val) != dict:
                        q[key] = {'$eq': val}
                    
                    if type(self._query[key]) != dict:
                        q[key]['$eq'] = self._query[key]
                    else:
                        q[key].update(self._query[key])
    
            for key, val in self._query.items():
                if key not in q:
                    q[key] = val
        else:
            q = self._query
        
        if self._indexes:
            return UdbCore.get_q_cursor(self, q, limit, offset, sort, get_plan, get_keys_only, use_indexes)

        return self._udb.get_q_cursor(self, q, limit, offset, sort, get_plan, get_keys_only, use_indexes)

    def _on_delete(self, rid):
        record = self._collection.get(rid)

        if record:
            for index in self._indexes.values():
                index.delete(index.get_cover_key(record), rid)

    def _on_insert(self, rid, values):
        for custom_check_condition in self._custom_check_condition:
            if not custom_check_condition(values, self._query):
                return

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.insert_is_allowed(index.get_cover_key(values))

        for index in self._indexes.values():
            index.insert_by_schema(values, rid)

    def _on_update(self, rid, before, values):
        for custom_check_condition in self._custom_check_condition:
            if not custom_check_condition(before, self._query, None, values):
                return self._on_delete(rid)

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.upsert_is_allowed(index.get_cover_key(before), index.get_cover_key(before, values))

        for index in self._indexes.values():
            index.upsert(index.get_cover_key(before), index.get_cover_key(before, values), rid)
