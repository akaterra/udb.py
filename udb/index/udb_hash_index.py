from ..common import EMPTY
from ..udb_index import UdbIndex, UdbEmbeddedIndex


class UdbHashIndex(UdbIndex):
    type = 'hash'

    def __init__(self, schema, name=None):
        UdbIndex.__init__(self, schema, name)

        self._hash = {}

    def __len__(self):
        return len(self._hash)

    def clear(self):
        self._hash.clear()

        return self

    def delete(self, key, uid=None):
        self._hash.pop(key)

        return self

    def insert(self, key, uid):
        self._hash[key] = uid

        return self

    def search_by_key(self, key):
        val = self._hash.get(key, EMPTY)

        if val != EMPTY:
            yield val

    def search_by_key_in(self, keys):
        for key in keys:
            val = self._hash.get(key, EMPTY)

            if val != EMPTY:
                yield val

    def upsert(self, old, new, uid):
        if old != new:
            self._hash.pop(old, None)

        self._hash[new] = uid

        return self


class UdbHashEmbeddedIndex(UdbHashIndex, UdbEmbeddedIndex):
    type = 'hash_embedded'

    def delete(self, key, uid=None):
        for key in key:
            self._hash.pop(key)

        return self

    def insert(self, key, uid):
        for key in key:
            self._hash[key] = uid

        return self

    def upsert(self, old, new, uid):
        self.delete(old)
        self.insert(new, uid)

        return self
