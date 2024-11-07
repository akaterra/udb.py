from ..common import EMPTY, FieldRequiredError, InvalidScanOperationValueError
from ..udb_index import UdbIndex, SCAN_OP_SEQ


def _q_arr_intersection(q):
    q.pop('$intersection')


def _q_arr_near(q):
    q.pop('$near')


SCAN_OP_INTERSECTION = 'intersection'
SCAN_OP_NEAR = 'near'


class UdbBaseGEOIndexCheckConditionContext(object):
    __slots__ = ('intersection', 'near', 'near_last', 'near_last_key')

    def __init__(self):
        self.intersection = self.near = self.near_last = self.near_last_key = None


class UdbBaseGEOIndexCheckConditionContextIntersection(object):
    __slots__ = ('x_min', 'x_max', 'y_min', 'y_max')

    def __init__(self):
        self.x_min = self.x_max = self.y_min = self.y_max = None


class UdbBaseGEOIndexCheckConditionContextNear(object):
    __slots__ = ('x', 'y', 'min_distance', 'max_distance')

    def __init__(self):
        self.x = self.y = self.min_distance = self.max_distance = None


EMPTY_DICT = {}


class UdbBaseGEOIndex(UdbIndex):
    is_prefixed = False
    is_ranged = False
    schema_last_index = 0

    _key = None
    _key_default_value = None

    _OPS = {
        '$intersection': None,
        '$near': None,
    }

    @classmethod
    def check_condition(cls, values, q, context=None, extend=None):
        if not context:
            context = cls.create_condition_context(q)

        if context.intersection:
            for key, c_intersection_q in context.intersection.items():
                val = values.get(key, EMPTY)

                if val is EMPTY and extend:
                    val = extend.get(key, EMPTY)

                is_acceptable = type(val) == list or type(val) == tuple

                if not is_acceptable:
                    return False

                if c_intersection_q.x_min > val[0]:
                    return False

                if c_intersection_q.x_max < val[0]:
                    return False

                if c_intersection_q.y_min > val[1]:
                    return False

                if c_intersection_q.y_max < val[1]:
                    return False

        if context.near:
            for key, c_near_q in context.near.items():
                val = values.get(key, None)
                is_acceptable = type(val) == list or type(val) == tuple

                if not is_acceptable:
                    return False

                c_x = c_near_q.x - val[0]
                c_y = c_near_q.y - val[1]
                distance = c_x * c_x + c_y * c_y

                if c_near_q.min_distance and c_near_q.min_distance > distance:
                    return False

                if c_near_q.max_distance and c_near_q.max_distance < distance:
                    return False

        return True

    @classmethod
    def create_condition_context(cls, q):
        context = UdbBaseGEOIndexCheckConditionContext()

        for key, condition in q.items():
            if condition and type(condition) == dict:
                c_intersection = condition.get('$intersection')

                if c_intersection:
                    if not context:
                        context = UdbBaseGEOIndexCheckConditionContext()

                    if not context.intersection:
                        context.intersection = {}

                    c_intersection_q = context.intersection[key] = UdbBaseGEOIndexCheckConditionContextIntersection()

                    c_intersection_q.x_min = c_intersection['xMin']
                    c_intersection_q.x_max = c_intersection['xMax']
                    c_intersection_q.y_min = c_intersection['yMin']
                    c_intersection_q.y_max = c_intersection['yMax']

                c_near = condition.get('$near')

                if c_near:
                    if not context:
                        context = UdbBaseGEOIndexCheckConditionContext()

                    if not context.near:
                        context.near = {}

                    c_near_q = context.near_last = context.near[key] = UdbBaseGEOIndexCheckConditionContextNear()

                    c_near_q.x = c_near['x']
                    c_near_q.y = c_near['y']
                    c_near_q.min_distance = c_near['minDistance'] ** 2 if 'minDistance' in c_near else None
                    c_near_q.max_distance = c_near['maxDistance'] ** 2 if 'maxDistance' in c_near else None

                if c_near:
                    context.near_last_key = key

        return context

    @classmethod
    def merge_condition(cls, q1, q2, context=None, extend=None):
        return q1

    @classmethod
    def seq(cls, seq, q, collection, schema=None):
        has_condition = False

        for cnd in q.values():
            if type(cnd) == dict and ('$intersection' in cnd or '$near' in cnd):
                has_condition = True
                break

        if not has_condition:
            return seq

        context = cls.create_condition_context(q)

        if context.near_last:
            def seq_q():
                for rid in seq:
                    if cls.check_condition(collection[rid], q, context):
                        yield rid

            # from nearest to far
            def seq_sort(rid):
                c_near = collection[rid][context.near_last_key]

                return (c_near[0] - context.near_last.x) ** 2 + (c_near[1] - context.near_last.y) ** 2

            def gen():
                for rid in sorted(seq_q(), key=seq_sort):
                    yield rid

            return gen()
        else:
            def gen():
                for rid in seq:
                    if cls.check_condition(collection[rid], q, context):
                        yield rid

            return gen()

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
                        1,
                        3,
                        lambda _: self.search_by_near(
                            c_near['x'],
                            c_near['y'],
                            c_near.get('minDistance'),
                            c_near.get('maxDistance'),
                            limit,
                            collection,
                        ),
                        _q_arr_near
                    )

        return SCAN_OP_SEQ, 0, 0, 0, None, None

    def has_key(self, key):
        return self._key == key

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
