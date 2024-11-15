from ..common import CHAR255, EMPTY, TYPE_INFL, TYPE_INFR
from .udb_base_linear_index import UdbBaseLinearIndex, UdbBaseLinearEmbeddedIndex


class UdbBtreeIndex(UdbBaseLinearIndex):
    is_ranged = True
    is_multivalued = True
    is_prefixed = True
    is_sorted_asc = True
    type = 'btree'

    def __init__(self, schema, name=None):
        from BTrees.OOBTree import OOBTree

        UdbBaseLinearIndex.__init__(self, schema, name)

        self._btree = OOBTree()

    def __len__(self):
        return len(self._btree)

    def clear(self):
        self._btree.clear()

        return self

    def delete(self, key_or_keys, uid):
        old_existing = self._btree.get(key_or_keys, EMPTY)

        if old_existing != EMPTY and uid in old_existing:
            if len(old_existing) == 1:
                self._btree.pop(key_or_keys)
            else:
                old_existing.remove(uid)

        return self

    def insert(self, key_or_keys, uid):
        old_existing = self._btree.get(key_or_keys, EMPTY)

        if old_existing == EMPTY:
            self._btree.insert(key_or_keys, {uid})
        else:
            old_existing.add(uid)

        return self

    def search_by_key_eq(self, key):
        val = self._btree.get(key, EMPTY)

        if val != EMPTY:
            for _ in val:
                yield _

    def search_by_key_ne(self, key):
        for val in self._btree.values(TYPE_INFL, key, False, True):
            for _ in val:
                yield _

        for val in self._btree.values(key, TYPE_INFR, True, False):
            for _ in val:
                yield _

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._btree.get(key, EMPTY)

            if val != EMPTY:
                for _ in val:
                    yield _

    def search_by_key_nin(self, keys):
        keys = list(keys)

        if len(keys) > 1:
            keys = list(sorted(keys))

            for val in self._btree.values(TYPE_INFL, keys[0], False, True):
                for _ in val:
                    yield _

            for i in range(len(keys) - 1):
                for val in self._btree.values(keys[i], keys[i + 1], True, True):
                    for _ in val:
                        yield _

            for val in self._btree.values(keys[-1], TYPE_INFR, True, False):
                for _ in val:
                    yield _
        else:
            for val in self._btree.values(TYPE_INFL, keys[0], False, True):
                for _ in val:
                    yield _

            for val in self._btree.values(keys[0], TYPE_INFR, True, False):
                for _ in val:
                    yield _

    def search_by_key_prefix(self, key):
        for val in self._btree.values(key, key + CHAR255):
            for _ in val:
                yield _

    def search_by_key_prefix_in(self, keys):
        keys = list(keys)
        min_key = min(*keys)
        max_key = max(*keys)

        for val in self._btree.values(min_key, max_key + TYPE_INFR):
            for _ in val:
                yield _

    def search_by_key_range(self, gte=None, lte=None, gte_excluded=False, lte_excluded=False):
        for val in self._btree.values(gte, lte, gte_excluded, lte_excluded):
            for _ in val:
                yield _

    def upsert(self, old, new, uid):
        if old != new:
            old_existing = self._btree.get(old, EMPTY)

            if old_existing != EMPTY and uid in old_existing:
                if len(old_existing) == 1:
                    self._btree.pop(old)
                else:
                    old_existing.remove(uid)

        new_existing = self._btree.get(new, EMPTY)

        if new_existing == EMPTY:
            self._btree.insert(new, {uid})
        else:
            new_existing.add(uid)

        return self


class UdbBtreeEmbeddedIndex(UdbBtreeIndex, UdbBaseLinearEmbeddedIndex):
    type = 'btree_embedded'

    def delete(self, key_or_keys, uid=None):
        for key in key_or_keys:
            old_existing = self._btree.get(key, EMPTY)

            if old_existing != EMPTY and uid in old_existing:
                if len(old_existing) == 1:
                    self._btree.pop(key)
                else:
                    old_existing.remove(uid)

        return self

    def insert(self, key_or_keys, uid):
        for key in key_or_keys:
            old_existing = self._btree.get(key, EMPTY)

            if old_existing == EMPTY:
                self._btree.insert(key, {uid})
            else:
                old_existing.append(uid)

        return self

    def upsert(self, old, new, uid):
        self.delete(old)
        self.insert(new, uid)

        return self
