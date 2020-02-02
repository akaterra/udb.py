from ..common import EMPTY
from ..udb_index import SCAN_OP_INTERSECTION, SCAN_OP_NEAR, SCAN_OP_SEQ, FieldRequiredError, InvalidScanOperationValueError
from .udb_base_geo_index import UdbBaseGEOIndex


class UdbRtreeIndex(UdbBaseGEOIndex):
    is_prefixed = False
    is_ranged = False
    type = 'rtree'

    def __init__(self, key, default_value=EMPTY, name=None):
        from rtree import index

        UdbBaseGEOIndex.__init__(self, key, default_value, name)

        self._rtree = index.Index()

    def __len__(self):
        return 0

    def clear(self):
        # @todo

        return self

    def delete(self, key, uid=None):
        self._rtree.delete(uid, (key[0], key[1], key[0], key[1]))

        return self

    def insert(self, key, uid):
        self._rtree.insert(uid, (key[0], key[1], key[0], key[1]))

        return self

    def search_by_intersection(self, p_x_min, p_y_min, p_x_max, p_y_max):
        return self._rtree.intersection((p_x_min, p_y_min, p_x_max, p_y_max))

    def search_by_near(self, p_x, p_y, min_distance=None, max_distance=None, limit=None, collection=None):
        if limit is None:
            limit = 999999999  # as unlimited for rtree.nearest
        elif limit < 1:
            return

        if min_distance or max_distance:
            if min_distance:
                min_distance **= 2

            if max_distance:
                max_distance **= 2

            # if there is a min distance and no max distance, select unlimited points since it is an unknown count of
            # points have to be skipped with a distance less then min distance
            iterator = self._rtree.nearest((p_x, p_y), 999999999 if min_distance and max_distance is None else limit)

            for val in iterator:
                doc = collection[val][self._key]
                c_x = p_x - doc[0]
                c_y = p_y - doc[1]

                distance = c_x * c_x + c_y * c_y

                if max_distance and max_distance < distance:
                    break

                if min_distance and min_distance > distance:
                    continue

                yield val

                if limit != - 1:
                    limit -= 1

                    if limit <= 0:
                        break
        else:
            for val in self._rtree.nearest((p_x, p_y), limit):
                yield val

    def upsert(self, old, new, uid):
        if old != new:
            self._rtree.delete(uid, (old[0], old[1], old[0], old[1]))

        self._rtree.insert(uid, (new[0], new[1], new[0], new[1]))

        return self
