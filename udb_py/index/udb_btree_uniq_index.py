from ..common import ConstraintError
from .udb_btree_base_index import UdbBtreeBaseIndex


class UdbBtreeUniqBaseIndex(UdbBtreeBaseIndex):
    is_uniq = True
    type = 'btree_uniq'

    def insert(self, key, uid):
        if key in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        self._btree.insert(key, uid)

        return self

    def insert_is_allowed(self, key):
        if key in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        return True

    def upsert(self, old, new, uid):
        if old != new:
            if new in self._btree:
                raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

            self._btree.pop(old)

        self._btree.insert(new, uid)

        return self

    def upsert_is_allowed(self, old, new):
        if old != new and new in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

        return True
