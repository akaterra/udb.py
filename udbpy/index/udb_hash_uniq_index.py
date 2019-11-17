from ..common import ConstraintError
from .udb_hash_index import UdbHashIndex


class UdbHashUniqIndex(UdbHashIndex):
    is_uniq = True
    type = 'hash_uniq'

    def insert(self, key, uid):
        if key in self._hash:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        self._hash[key] = uid

        return self

    def insert_is_allowed(self, key):
        if key in self._hash:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        return True

    def upsert(self, old, new, uid):
        if old != new:
            if new in self._hash:
                raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

            self._hash.pop(old)

        self._hash[new] = uid

        return self

    def upsert_is_allowed(self, old, new):
        if old != new and new in self._hash:
            raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

        return True
