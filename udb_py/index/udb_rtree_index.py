from ..common import EMPTY
from ..udb_index import SCAN_OP_INTERSECTION, SCAN_OP_NEAR, SCAN_OP_SEQ, FieldRequiredError, InvalidScanOperationValueError
from .udb_base_geo_index import UdbBaseGEOIndex


# def _q_arr_intersection(q):
#     q.pop('$intersection')


# def _q_arr_near(q):
#     q.pop('$near')


class UdbRtreeIndex(UdbBaseGEOIndex):
    # schema_keys = []
    is_prefixed = False
    is_ranged = False
    # schema_last_index = 0
    type = 'rtree'

    # _key = None
    # _key_default_value = None

    # _OPS = {
    #     '$intersection': None,
    #     '$near': None,
    # }

    # @classmethod
    # def seq(cls, seq, q, collection):
    #     for key, condition in q.items():
    #         if type(condition) == dict:
    #             c_near = condition.get('$near')

    #             if c_near:
    #                 c_near_x = c_near['x']
    #                 c_near_y = c_near['y']
    #                 c_near_max_distance = c_near.get('maxDistance', None)
    #                 c_near_min_distance = c_near.get('minDistance', None)

    #                 if c_near_max_distance is not None:
    #                     c_near_max_distance **= 2

    #                 if c_near_min_distance is not None:
    #                     c_near_min_distance **= 2

    #                 def seq_q(seq_key, _):
    #                     for rid in seq:
    #                         doc = collection[rid][seq_key]
    #                         c_x = c_near_x - doc[0]
    #                         c_y = c_near_y - doc[1]

    #                         distance = c_x * c_x + c_y * c_y

    #                         if c_near_max_distance and c_near_max_distance < distance:
    #                             continue

    #                         if c_near_min_distance and c_near_min_distance > distance:
    #                             continue

    #                         yield rid

    #                 # from nearest to far
    #                 def seq_sort(seq_key):
    #                     record = collection[seq_key]

    #                     return (record[seq_key][0] - c_near['x']) ** 2 + (record[seq_key][1] - c_near['y']) ** 2

    #                 return sorted(seq_q(key, condition), key=seq_sort)

    #             c_intersection = condition.get('$intersection')

    #             if c_intersection:
    #                 c_intersection_x_min = c_intersection['xMin']
    #                 c_intersection_y_min = c_intersection['yMin']
    #                 c_intersection_x_max = c_intersection['xMax']
    #                 c_intersection_y_max = c_intersection['yMax']

    #                 def seq_q(seq_key, _):
    #                     for rid in seq:
    #                         record = collection[rid][seq_key]

    #                         if c_intersection_x_min <= record[0] <= c_intersection_x_max:
    #                             if c_intersection_y_min <= record[1] <= c_intersection_y_max:
    #                                 yield rid

    #                 return seq_q(key, condition)

    #     return seq

    # @classmethod
    # def validate_query(cls, q):
    #     for key, condition in q.items():
    #         if type(condition) == dict:
    #             for op_key, op_condition in condition.items():
    #                 if op_key == '$intersection':
    #                     if type(op_condition) != dict:
    #                         raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))

    #                     if type(op_condition.get('xMin')) != float and type(op_condition.get('xMin')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.xMin'.format(key, op_key))

    #                     if type(op_condition.get('yMin')) != float and type(op_condition.get('yMin')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.yMin'.format(key, op_key))

    #                     if type(op_condition.get('xMax')) != float and type(op_condition.get('xMax')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.xMax'.format(key, op_key))

    #                     if type(op_condition.get('yMax')) != float and type(op_condition.get('yMax')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.yMax'.format(key, op_key))
    #                 elif op_key == '$near':
    #                     if type(op_condition) != dict:
    #                         raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))

    #                     if type(op_condition.get('x')) != float and type(op_condition.get('x')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.x'.format(key, op_key))

    #                     if type(op_condition.get('y')) != float and type(op_condition.get('y')) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.y'.format(key, op_key))

    #                     max_distance = op_condition.get('maxDistance', EMPTY)

    #                     if max_distance != EMPTY and type(max_distance) != float and type(max_distance) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.maxDistance'.format(key, op_key))

    #                     min_distance = op_condition.get('minDistance', EMPTY)

    #                     if min_distance != EMPTY and type(min_distance) != float and type(min_distance) != int:
    #                         raise InvalidScanOperationValueError('{}.{}.minDistance'.format(key, op_key))

    #     return True

    def __init__(self, key, default_value=EMPTY, name=None):
        from rtree import index

        UdbBaseGEOIndex.__init__(self, key, default_value, name)

        # self.schema = {key: default_value}
        # self.schema_keys = [key]
        # self._key = key
        # self._key_default_value = default_value
        self._rtree = index.Index()

    def __len__(self):
        return 0

    # def get_cover_key(self, record, second=None):
    #     get = self._key_default_value

    #     if callable(get):
    #         val = get(self._key, record)
    #     elif second:
    #         val = second.get(self._key, get)

    #         if val == EMPTY:
    #             val = record.get(self._key, get)
    #     else:
    #         val = record.get(self._key, get)

    #     if val != EMPTY:
    #         return val

    #     return None

    # def get_cover_key_or_raise(self, record, second=None):
    #     get = self._key_default_value

    #     if callable(get):
    #         val = get(self._key, record)
    #     elif second:
    #         val = second.get(self._key, get)

    #         if val == EMPTY:
    #             val = record.get(self._key, get)
    #     else:
    #         val = record.get(self._key, get)

    #     if val != EMPTY:
    #         return val

    #     raise FieldRequiredError('field required: {} on {}'.format(self._key, self.name))

    # def get_scan_op(self, q, limit=None, offset=None, collection=None):
    #     for key in self.schema_keys:
    #         condition = q.get(key, EMPTY)

    #         if type(condition) == dict:
    #             c_near = condition.get('$near')

    #             if c_near:
    #                 return (
    #                     SCAN_OP_NEAR,
    #                     1,
    #                     3,
    #                     lambda _: self.search_by_near(
    #                         c_near['x'],
    #                         c_near['y'],
    #                         c_near.get('minDistance'),
    #                         c_near.get('maxDistance'),
    #                         limit,
    #                         collection
    #                     ),
    #                     _q_arr_near
    #                 )

    #             c_intersection = condition.get('$intersection')

    #             if c_intersection:
    #                 return (
    #                     SCAN_OP_INTERSECTION,
    #                     1,
    #                     3,
    #                     lambda _: self.search_by_intersection(
    #                         c_intersection['minX'],
    #                         c_intersection['minY'],
    #                         c_intersection['maxX'],
    #                         c_intersection['maxY'],
    #                     ),
    #                     _q_arr_intersection
    #                 )

    #     return SCAN_OP_SEQ, 0, 0, None, None

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
            limit = 99999999  # as unlimited for rtree.nearest
        elif limit < 1:
            return

        if min_distance:
            min_distance **= 2

        if max_distance:
            max_distance **= 2

        if min_distance or max_distance:
            # if there is a min distance and no max distance, select unlimited points since it is an unknown count of
            # points have to be skipped with a distance less then min distance
            iterator = self._rtree.nearest((p_x, p_y), 99999999 if min_distance and max_distance is None else limit)

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
