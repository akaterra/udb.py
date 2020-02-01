from ..common import CHAR255, EMPTY
from .udb_base_linear_index import UdbBaseLinearIndex, UdbBaseLinearEmbeddedIndex


class UdbBtreeMultivaluedIndex(UdbBaseLinearIndex):
    is_ranged = True
    is_multivalued = True
    is_prefixed = True
    is_sorted_asc = True
    type = 'btree_multivalued'

    def __init__(self, schema, name=None):
        from BTrees.OOBTree import OOBTree

        UdbBaseLinearIndex.__init__(self, schema, name)

        self._btree = OOBTree()

    def __len__(self):
        return len(self._btree)

    def clear(self):
        self._btree.clear()

        return self

    def delete(self, key, uid):
        old_existing = self._btree.get(key, EMPTY)

        if old_existing != EMPTY and uid in old_existing:
            if len(old_existing) == 1:
                self._btree.pop(key)
            else:
                old_existing.remove(uid)

        return self

    def insert(self, key, uid):
        old_existing = self._btree.get(key, EMPTY)

        if old_existing == EMPTY:
            self._btree.insert(key, {uid})
        else:
            old_existing.add(uid)

        return self

    def search_by_key(self, key):
        val = self._btree.get(key, EMPTY)

        if val != EMPTY:
            for _ in val:
                yield _

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._btree.get(key, EMPTY)

            if val != EMPTY:
                for _ in val:
                    yield _

    def search_by_key_prefix(self, key):
        for val in self._btree.values(key, key + CHAR255):
            for _ in val:
                yield _

    def search_by_key_prefix_in(self, keys):
        for key in keys:
            for val in self._btree.values(key, key + CHAR255):
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


class UdbBtreeMultivaluedEmbeddedIndex(UdbBtreeMultivaluedIndex, UdbBaseLinearEmbeddedIndex):
    type = 'btree_multivalued_embedded'

    def delete(self, key, uid=None):
        for key in key:
            old_existing = self._btree.get(key, EMPTY)

            if old_existing != EMPTY and uid in old_existing:
                if len(old_existing) == 1:
                    self._btree.pop(key)
                else:
                    old_existing.remove(uid)

        return self

    def insert(self, key, uid):
        for key in key:
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
