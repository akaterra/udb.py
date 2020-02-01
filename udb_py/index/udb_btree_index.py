from ..common import CHAR255, EMPTY
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

    def delete(self, key, uid=None):
        self._btree.pop(key, EMPTY)

        return self

    def insert(self, key, uid):
        self._btree.insert(key, uid)

        return self

    def search_by_key(self, key):
        val = self._btree.get(key, EMPTY)

        if val != EMPTY:
            yield val

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._btree.get(key, EMPTY)

            if val != EMPTY:
                yield val

    def search_by_key_prefix(self, key):
        for val in self._btree.values(key, key + CHAR255):
            yield val

    def search_by_key_prefix_in(self, keys):
        for key in keys:
            for val in self._btree.values(key, key + CHAR255):
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

    def delete(self, key, uid=None):
        for key in key:
            self._btree.pop(key, EMPTY)

        return self

    def insert(self, key, uid):
        for key in key:
            self._btree.insert(key, uid)

        return self

    def upsert(self, old, new, uid):
        self.delete(old)
        self.insert(new, uid)

        return self
