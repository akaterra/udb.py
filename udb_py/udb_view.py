from .udb_core import UdbCore, cpy_dict


class UdbView(UdbCore):
    _query = None

    def __init__(self, udb, query, indexes=UdbCore):
        if not indexes:
            indexes = {}
        elif indexes is UdbCore:
            indexes = cpy_dict(udb.indexes)

        UdbCore.__init__(self, indexes)

        self._collection = udb.collection
        self._query = query

        udb\
            .add_on_delete(self._on_delete)\
            .add_on_insert(self._on_insert)\
            .add_on_update(self._on_update)

        for rid in udb.get_q_cursor(cpy_dict(query), get_keys_only=True):
            self._on_insert(rid, self._collection[rid])

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
            if not custom_check_condition(values, self._query):
                return

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.upsert_is_allowed(index.get_cover_key(before), index.get_cover_key(before, values))

        for index in self._indexes.values():
            index.upsert(index.get_cover_key(before), index.get_cover_key(before, values), rid)
