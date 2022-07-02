from ..common import CHAR255, EMPTY, TYPE_INFL, TYPE_INFR, sort_iter
from .udb_base_linear_index import UdbBaseLinearIndex, UdbBaseLinearEmbeddedIndex


class UdbBtreeIndex(UdbBaseLinearIndex):
    is_prefixed = True
    is_ranged = True
    is_sorted_asc = True
    type = 'btree'

    def __init__(self, schema, name=None):
        from BTrees.OIBTree import OIBTree

        UdbBaseLinearIndex.__init__(self, schema, name)

        self._btree = OIBTree()

    def __len__(self):
        return len(self._btree)

    def clear(self):
        self._btree.clear()

        return self

    def delete(self, key_or_keys, uid=None):
        self._btree.pop(key_or_keys, EMPTY)

        return self

    def insert(self, key_or_keys, uid):
        self._btree.insert(key_or_keys, uid)

        return self

    def search_by_key_eq(self, key):
        val = self._btree.get(key, EMPTY)

        if val != EMPTY:
            yield val

    def search_by_key_ne(self, key):
        for val in self._btree.values(TYPE_INFL, key, False, True):
            yield val

        for val in self._btree.values(key, TYPE_INFR, True, False):
            yield val

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._btree.get(key, EMPTY)

            if val != EMPTY:
                yield val

    def search_by_key_nin(self, keys):
        keys = list(keys)

        if len(keys) > 1:
            keys = list(sorted(keys))

            for val in self._btree.values(TYPE_INFL, keys[0], False, True):
                yield val

            for i in range(len(keys) - 1):
                for val in self._btree.values(keys[i], keys[i + 1], True, True):
                    yield val

            for val in self._btree.values(keys[-1], TYPE_INFR, True, False):
                yield val
        else:
            for val in self._btree.values(TYPE_INFL, keys[0], False, True):
                yield val

            for val in self._btree.values(keys[0], TYPE_INFR, True, False):
                yield val

    def search_by_key_prefix(self, key):
        for val in self._btree.values(key, key + TYPE_INFR):
            yield val

    def search_by_key_prefix_in(self, keys):
        for key in keys:
            for val in self._btree.values(key, key + TYPE_INFR):
                yield val

    def search_by_key_range(self, gte=None, lte=None, gte_excluded=False, lte_excluded=False):
        for val in self._btree.values(gte, lte, gte_excluded, lte_excluded):
            yield val

    def upsert(self, old, new, uid):
        if old != new:
            self._btree.pop(old)

        self._btree.insert(new, uid)

        return self


class UdbBtreeEmbeddedIndex(UdbBtreeIndex, UdbBaseLinearEmbeddedIndex):
    type = 'btree_embedded'

    def delete(self, key_or_keys, uid=None):
        for key in key_or_keys:
            self._btree.pop(key, EMPTY)

        return self

    def insert(self, key_or_keys, uid):
        for key in key_or_keys:
            self._btree.insert(key, uid)

        return self

    def upsert(self, old, new, uid):
        self.delete(old)
        self.insert(new, uid)

        return self
