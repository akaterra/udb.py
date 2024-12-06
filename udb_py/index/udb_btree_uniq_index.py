from ..common import ConstraintError
from .udb_btree_base_index import UdbBtreeBaseIndex


class UdbBtreeUniqIndex(UdbBtreeBaseIndex):
    is_uniq = True
    type = 'btree_uniq'

    def clone(self):
        return UdbBtreeUniqIndex(self.schema, self.name).safe(self._safe)

    def insert(self, key, rid):
        if key in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        self._btree.insert(key, rid)

        return self

    def insert_is_allowed(self, key):
        if key in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(key, self.name))

        return True

    def upsert(self, old, new, rid, q=None):
        if old != new:
            if new in self._btree:
                raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

            self._btree.pop(old)

        self._btree.insert(new, rid)

        return self

    def upsert_is_allowed(self, old, new, q=None):
        if old != new and new in self._btree:
            raise ConstraintError('duplicate value: {} on {}'.format(new, self.name))

        return True
