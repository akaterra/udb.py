from ..common import EMPTY
from ..udb_index import UdbIndex, SCAN_OP_INTERSECTION, SCAN_OP_NEAR, SCAN_OP_SEQ, FieldRequiredError, InvalidScanOperationValueError


def _q_arr_intersection(q):
    q.pop('$intersection')


def _q_arr_near(q):
    q.pop('$near')


class UdbBaseGEOIndexCheckConditionContext(object):
    __slots__ = ('intersection', 'near')


class UdbBaseGEOIndexCheckConditionContextIntersection(object):
    __slots__ = ('x_min', 'x_max', 'y_min', 'y_max')


class UdbBaseGEOIndexCheckConditionContextNear(object):
    __slots__ = ('x', 'y', 'min_distance', 'max_distance')


class UdbBaseGEOIndex(UdbIndex):
    is_prefixed = False
    is_ranged = False

    _key = None
    _key_default_value = None

    _OPS = {
        '$intersection': None,
        '$near': None,
    }

    @classmethod
    def check_condition(cls, record, q, context=None):
        for key, condition in q.items():
            record_value = record.get(key, None)
            is_record_acceptable = type(record_value) == list or type(record_value) == tuple

            if condition and type(condition) == dict:
                c_intersection = condition.get('$intersection')

                if c_intersection:
                    if not is_record_acceptable:
                        return False

                    if not context:
                        context = UdbBaseGEOIndexCheckConditionContext()

                        context.intersection = {}
                    elif not context.intersection:
                        context.intersection = {}

                    c_intersection_q = context.intersection.get(key)

                    if not c_intersection_q:
                        c_intersection_q = context.intersection[key] = UdbBaseGEOIndexCheckConditionContextIntersection()

                        c_intersection_q.x_min = c_intersection['xMin']
                        c_intersection_q.x_max = c_intersection['xMax']
                        c_intersection_q.y_min = c_intersection['yMin']
                        c_intersection_q.y_max = c_intersection['yMax']

                    if c_intersection_q.x_min > record_value[0]:
                        return False

                    if c_intersection_q.x_max < record_value[0]:
                        return False

                    if c_intersection_q.y_min > record_value[1]:
                        return False

                    if c_intersection_q.y_max < record_value[1]:
                        return False

                c_near = condition.get('$near')

                if c_near:
                    if not is_record_acceptable:
                        return False

                    if not context:
                        context = UdbBaseGEOIndexCheckConditionContext()

                        context.near = {}
                    elif not context.near:
                        context.near = {}

                    c_near_q = context.near.get(key)

                    if not c_near_q:
                        c_near_q = context.near[key] = UdbBaseGEOIndexCheckConditionContextNear()

                        c_near_q.x = c_near['x']
                        c_near_q.y = c_near['y']
                        c_near_q.min_distance = c_near['minDistance']**2 if 'minDistance' in c_near else None
                        c_near_q.max_distance = c_near['maxDistance']**2 if 'maxDistance' in c_near else None

                    c_x = c_near_q.x - record_value[0]

                    c_y = c_near_q.y - record_value[1]

                    distance = c_x * c_x + c_y * c_y

                    if c_near_q.min_distance and c_near_q.min_distance > distance:
                        return False

                    if c_near_q.max_distance and c_near_q.max_distance < distance:
                        return False

        return True

    @classmethod
    def seq(cls, seq, q, collection):
        context = UdbBaseGEOIndexCheckConditionContext()

        for rid in seq:
            if cls.check_condition(collection[rid], q, context):
                yield rid

    @classmethod
    def validate_query(cls, q):
        for key, condition in q.items():
            if type(condition) == dict:
                for op_key, op_condition in condition.items():
                    if op_key == '$intersection':
                        if type(op_condition) != dict:
                            raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))

                        if type(op_condition.get('xMin')) != float and type(op_condition.get('xMin')) != int:
                            raise InvalidScanOperationValueError('{}.{}.xMin'.format(key, op_key))

                        if type(op_condition.get('yMin')) != float and type(op_condition.get('yMin')) != int:
                            raise InvalidScanOperationValueError('{}.{}.yMin'.format(key, op_key))

                        if type(op_condition.get('xMax')) != float and type(op_condition.get('xMax')) != int:
                            raise InvalidScanOperationValueError('{}.{}.xMax'.format(key, op_key))

                        if type(op_condition.get('yMax')) != float and type(op_condition.get('yMax')) != int:
                            raise InvalidScanOperationValueError('{}.{}.yMax'.format(key, op_key))
                    elif op_key == '$near':
                        if type(op_condition) != dict:
                            raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))

                        if type(op_condition.get('x')) != float and type(op_condition.get('x')) != int:
                            raise InvalidScanOperationValueError('{}.{}.x'.format(key, op_key))

                        if type(op_condition.get('y')) != float and type(op_condition.get('y')) != int:
                            raise InvalidScanOperationValueError('{}.{}.y'.format(key, op_key))

                        max_distance = op_condition.get('maxDistance', EMPTY)

                        if max_distance != EMPTY and type(max_distance) != float and type(max_distance) != int:
                            raise InvalidScanOperationValueError('{}.{}.maxDistance'.format(key, op_key))

                        min_distance = op_condition.get('minDistance', EMPTY)

                        if min_distance != EMPTY and type(min_distance) != float and type(min_distance) != int:
                            raise InvalidScanOperationValueError('{}.{}.minDistance'.format(key, op_key))

        return True

    def __init__(self, key, default_value=EMPTY, name=None):
        from rtree import index

        UdbIndex.__init__(self, name)

        self.schema = {key: default_value}
        self.schema_keys = [key]
        self._key = key
        self._key_default_value = default_value

    def __len__(self):
        return 0

    def get_cover_key(self, record, second=None):
        get = self._key_default_value

        if callable(get):
            val = get(self._key, record)
        elif second:
            val = second.get(self._key, get)

            if val == EMPTY:
                val = record.get(self._key, get)
        else:
            val = record.get(self._key, get)

        if val != EMPTY:
            return val

        return None

    def get_cover_key_or_raise(self, record, second=None):
        get = self._key_default_value

        if callable(get):
            val = get(self._key, record)
        elif second:
            val = second.get(self._key, get)

            if val == EMPTY:
                val = record.get(self._key, get)
        else:
            val = record.get(self._key, get)

        if val != EMPTY:
            return val

        raise FieldRequiredError('field required: {} on {}'.format(self._key, self.name))

    def get_meta(self):
        return {'key': self._key, 'default_value': self._key_default_value, 'name': self.name}

    def get_scan_op(self, q, limit=None, offset=None, collection=None):
        for key in self.schema_keys:
            condition = q.get(key, EMPTY)

            if type(condition) == dict:
                c_intersection = condition.get('$intersection')

                if c_intersection:
                    return (
                        SCAN_OP_INTERSECTION,
                        1,
                        3,
                        lambda _: self.search_by_intersection(
                            c_intersection['minX'],
                            c_intersection['minY'],
                            c_intersection['maxX'],
                            c_intersection['maxY'],
                        ),
                        _q_arr_intersection
                    )

                c_near = condition.get('$near')

                if c_near:
                    return (
                        SCAN_OP_NEAR,
                        1,
                        3,
                        lambda _: self.search_by_near(
                            c_near['x'],
                            c_near['y'],
                            c_near.get('minDistance'),
                            c_near.get('maxDistance'),
                            limit,
                            collection
                        ),
                        _q_arr_near
                    )

        return SCAN_OP_SEQ, 0, 0, None, None

    def clear(self):
        raise NotImplementedError

    def delete(self, key, uid=None):
        raise NotImplementedError

    def insert(self, key, uid):
        raise NotImplementedError

    def search_by_intersection(self, p_x_min, p_y_min, p_x_max, p_y_max):
        raise NotImplementedError

    def search_by_near(self, p_x, p_y, min_distance=None, max_distance=None, limit=None, collection=None):
        raise NotImplementedError

    def upsert(self, old, new, uid):
        raise NotImplementedError
