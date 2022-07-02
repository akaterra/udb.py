from ..common import EMPTY
from .udb_base_linear_index import UdbBaseLinearIndex, UdbBaseLinearEmbeddedIndex


class UdbHashMultivaluedIndex(UdbBaseLinearIndex):
    is_multivalued = True
    type = 'hash_multivalued'

    def __init__(self, schema, name=None):
        UdbBaseLinearIndex.__init__(self, schema, name)

        self._hash = {}

    def __len__(self):
        return len(self._hash)

    def clear(self):
        self._hash.clear()

        return self

    def delete(self, key, uid):
        old_existing = self._hash.get(key, EMPTY)

        if old_existing != EMPTY and uid in old_existing:
            if len(old_existing) == 1:
                self._hash.pop(key)
            else:
                old_existing.remove(uid)

        return self

    def insert(self, key, uid):
        old_existing = self._hash.get(key, EMPTY)

        if old_existing == EMPTY:
            self._hash[key] = {uid}
        else:
            old_existing.add(uid)

        return self

    def search_by_key_eq(self, key):
        val = self._hash.get(key, EMPTY)

        if val != EMPTY:
            for _ in val:
                yield _

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._hash.get(key, EMPTY)

            if val != EMPTY:
                for _ in val:
                    yield _

    def upsert(self, old, new, uid):
        if old != new:
            old_existing = self._hash.get(old, EMPTY)

            if old_existing != EMPTY and uid in old_existing:
                if len(old_existing) == 1:
                    self._hash.pop(old)
                else:
                    old_existing.remove(uid)

        new_existing = self._hash.get(new, EMPTY)

        if new_existing == EMPTY:
            self._hash[new] = {uid}
        else:
            new_existing.add(uid)

        return self


class UdbHashMultivaluedEmbeddedIndex(UdbHashMultivaluedIndex, UdbBaseLinearEmbeddedIndex):
    type = 'hash_multivalued_embedded'

    def delete(self, key_or_keys, uid=None):
        for key in key_or_keys:
            old_existing = self._hash.get(key, EMPTY)

            if old_existing != EMPTY and uid in old_existing:
                if len(old_existing) == 1:
                    self._hash.pop(key)
                else:
                    old_existing.remove(uid)

        return self

    def insert(self, key_or_keys, uid):
        for key in key_or_keys:
            old_existing = self._hash.get(key, EMPTY)

            if old_existing == EMPTY:
                self._hash[key] = {uid}
            else:
                old_existing.append(uid)

        return self

    def upsert(self, old, new, uid):
        self.delete(old)
        self.insert(new, uid)

        return self
