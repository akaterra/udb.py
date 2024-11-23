import math
from ..common import EMPTY
from .udb_base_geo_index import UdbBaseGEOIndex


class UdbRtreeIndex(UdbBaseGEOIndex):
    is_prefixed = False
    is_ranged = False
    type = 'rtree'

    def __init__(self, key, default_value=EMPTY, name=None):
        from rtree import index

        UdbBaseGEOIndex.__init__(self, key, default_value, name)

        self._rtree = index.Index()
        self._x_min = None
        self._x_max = None
        self._y_min = None
        self._y_max = None

    def __len__(self):
        return 0

    def bounds(self):
        return self._x_min, self._y_min, self._x_max, self._y_max

    def clear(self):
        # TODO

        return self

    def clone(self):
        return UdbRtreeIndex(self._key, self._key_default_value, self.name).safe(self._safe)

    def rids(self):
        for rid in self._rtree.intersection((-math.inf, -math.inf, +math.inf, +math.inf)):
            yield rid

    def delete(self, key, uid=None, q=None):
        self._rtree.delete(uid, (key[0], key[1], key[0], key[1]))

        return self

    def insert(self, key, uid):
        self._rtree.insert(uid, (key[0], key[1], key[0], key[1]))

        if self._x_min is None:
            self._x_min = key[0]
            self._x_max = key[0]
            self._y_min = key[1]
            self._y_max = key[1]
        else:
            if self._x_min > key[0]:
                self._x_min = key[0]
            elif self._x_max < key[0]:
                self._x_max = key[0]

            if self._y_min > key[1]:
                self._y_min = key[1]
            elif self._y_max < key[1]:
                self._y_max = key[1]

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

    def upsert(self, old, new, uid, q=None):
        if old != new:
            self._rtree.delete(uid, (old[0], old[1], old[0], old[1]))

        self._rtree.insert(uid, (new[0], new[1], new[0], new[1]))

        if self._x_min is None:
            self._x_min = new[0]
            self._x_max = new[0]
            self._y_min = new[1]
            self._y_max = new[1]
        else:
            if self._x_min > new[0]:
                self._x_min = new[0]
            elif self._x_max < new[0]:
                self._x_max = new[0]

            if self._y_min > new[1]:
                self._y_min = new[1]
            elif self._y_max < new[1]:
                self._y_max = new[1]

        return self
