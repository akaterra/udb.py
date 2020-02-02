from .udb_core import UdbCore


class UdbView(UdbCore):
    _query = None

    def __init__(self, udb, query, indexes=None):
        if not indexes:
            indexes = {}

        indexes.update(udb.indexes)

        UdbCore.__init__(self, indexes)

        self._collection = udb.collection

        udb\
            .add_on_delete(self._on_delete)\
            .add_on_insert(self._on_insert)\
            .add_on_update(self._on_update)

    def _on_delete(self, key):
        pass

    def _on_insert(self, revision, values):
        if self._schema:
            for key, schema_entry in self._schema.items():
                if callable(schema_entry):
                    values[key] = schema_entry(key, values)
                elif key not in values:
                    values[key] = schema_entry

        if self._indexes_to_check_for_ins_upd_allowance:
            for index in self._indexes_to_check_for_ins_upd_allowance:
                index.insert_is_allowed(index.get_cover_key(values))

        for index in self._custom_indexes.values():
            index.insert_by_schema(values, revision)

        return values

    def _on_update(self, key, before, values):
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
