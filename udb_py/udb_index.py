import collections

from .common import (
    FieldRequiredError,
    InvalidScanOperationValueError,
    UnknownSeqScanOperationError,
    CHAR255,
    EMPTY,
    TYPE_COMPARERS,
    TYPE_FORMAT_MAPPERS,
    configure_float_precision,
)


def _q_arr_eq(q):
    q.pop('$eq')


def _q_arr_in(q):
    q.pop('$in')


def _q_arr_range(q):
    q.pop('$gt', q.pop('$gte', q.pop('$lt', q.pop('$lte', EMPTY))))


_PRIMITIVE_VALS = (None, bool, float, int, str)


SCAN_OP_CONST = 'const'
SCAN_OP_EMPTY = 'empty'
SCAN_OP_IN = 'in'
SCAN_OP_INTERSECTION = 'intersection'
SCAN_OP_NEAR = 'near'
SCAN_OP_PREFIX = 'prefix'
SCAN_OP_PREFIX_IN = 'prefix_in'
SCAN_OP_RANGE = 'range'
SCAN_OP_SEQ = 'seq'
SCAN_OP_SORT = 'sort'
SCAN_OP_SUB = 'sub'


def _eq_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a == b


def _gt_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a > b if can_be_compared is None else can_be_compared


def _gte_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a >= b if can_be_compared is None else can_be_compared


def _in_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a in b


def _lt_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a < b if can_be_compared is None else not can_be_compared


def _lte_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a <= b if can_be_compared is None else not can_be_compared


def _ne_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a != b


def _nin_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a not in b


class UdbIndex(object):
    is_embedded = False
    is_multivalued = False
    is_prefixed = False
    is_ranged = False
    is_sorted_asc = False
    is_sparsed = False
    is_uniq = False
    name = 'index'
    schema = {}
    schema_default_values = None
    schema_keys = []
    schema_last_index = - 1
    type = None
    type_format_mappers = TYPE_FORMAT_MAPPERS

    _OPS = {
        '$eq': _eq_op,
        '$gt': _gt_op,
        '$gte': _gte_op,
        '$in': _in_op,
        '$lt': _lt_op,
        '$lte': _lte_op,
        '$ne': _ne_op,
        '$nin': _nin_op,
    }

    @classmethod
    def seq(cls, seq, q, collection):
        for rid in seq:
            passed = True
            record = collection[rid]

            for key, condition in q.items():
                _ = record.get(key, None)

                if condition and type(condition) == dict:
                    for op_key, op_condition in condition.items():
                        op = cls._OPS.get(op_key)

                        if op:
                            passed = op(_, op_condition)

                        if not passed:
                            break
                else:
                    passed = _eq_op(_, condition)

                if not passed:
                    break

            if passed:
                yield rid

    @classmethod
    def validate_query(cls, q):
        for key, condition in q.items():
            if type(condition) == dict:
                for op_key, op_condition in condition.items():
                    if op_key in ('$in', '$nin'):
                        if type(op_condition) != list:
                            raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))

                        for ind, in_value in enumerate(op_condition):
                            if op_key in cls._OPS and in_value is not None and type(in_value) not in _PRIMITIVE_VALS:
                                raise InvalidScanOperationValueError('{}.{}[{}]'.format(key, op_key, ind))
                    elif op_key in cls._OPS and op_condition is not None and type(op_condition) not in _PRIMITIVE_VALS:
                        raise InvalidScanOperationValueError('{}.{}'.format(key, op_key))
            else:
                if condition is not None and type(condition) not in _PRIMITIVE_VALS:
                    raise InvalidScanOperationValueError(key)

        return True

    def __init__(self, schema=None, name=None, sparsed=False):
        """
        :param schema: Index schema as list of keys or set of keys and accessors.
        :param name:
        :param sparsed:
        """
        self.is_sparsed = sparsed
        self.name = name or type(self).__name__

        if type(schema) == list or type(schema) == tuple:
            schema = collections.OrderedDict(
                (v[0], v[1])
                if type(v) == list or type(v) == tuple
                else (v, EMPTY) for v in schema
            )

        if schema:
            self.schema = schema
            self.schema_default_values = {key: val for key, val in schema.items() if val != EMPTY}
            self.schema_keys = list(schema.keys())
            self.schema_last_index = len(schema) - 1

    def get_key(self, key, default=None):
        raise NotImplementedError

    def get_cover_key(self, record, second=None):
        key = ''
        type_format_mappers = self.type_format_mappers
        i = 0

        for k in self.schema_keys:
            get = self.schema[k]

            if callable(get):
                val = get(k, record)
            elif second:
                val = second.get(k, get)

                if val == EMPTY:
                    val = record.get(k, get)
            else:
                val = record.get(k, get)

            if val != EMPTY:
                key += type_format_mappers[type(val)](val)
            else:
                if self.is_sparsed:
                    if i > 0:
                        key += type_format_mappers[type(None)](None)
                    else:
                        return None
                else:
                    return None

            i += 1

        return key

    def get_cover_key_or_raise(self, record, second=None):
        key = ''
        type_format_mappers = self.type_format_mappers
        i = 0

        for k in self.schema_keys:
            get = self.schema[k]

            if callable(get):
                val = get(k, record)
            elif second:
                val = second.get(k, get)

                if val == EMPTY:
                    val = record.get(k, get)
            else:
                val = record.get(k, get)

            if val != EMPTY:
                key += type_format_mappers[type(val)](val)
            else:
                if self.is_sparsed:
                    if i > 0:
                        key += type_format_mappers[type(None)](None)
                    else:
                        return None
                else:
                    raise FieldRequiredError('field required: {} on {}'.format(k, self.name))

            i += 1

        return key

    def get_scan_op(self, q, limit=None, offset=None, collection=None):
        """
        Gets scan op for the coverage key.

        :param q:
        :param limit:
        :param offset:
        :param collection:

        :return: (
            op type,
            key sequence length to extract as prefix key,
            priority,
            fn,
            fn_q_arranger,
        )
        """
        type_format_mappers = self.type_format_mappers
        ind = - 1

        for ind, key in enumerate(self.schema_keys):
            condition = q.get(key, EMPTY)

            if condition == EMPTY:
                if not self.is_prefixed:
                    return SCAN_OP_SEQ, 0, 0, None, None

                if ind == 0:
                    return SCAN_OP_SEQ, 0, 0, None, None

                return SCAN_OP_PREFIX, ind, 1, self.search_by_key_prefix, None

            if type(condition) == dict:
                c_eq = condition.get('$eq', EMPTY)

                if c_eq != EMPTY:
                    if ind == self.schema_last_index:
                        return (
                            SCAN_OP_CONST,
                            ind + 1,  # cover key length
                            2,  # priority
                            lambda k: self.search_by_key(k + type_format_mappers[type(c_eq)](c_eq)),
                            _q_arr_eq
                        )

                    if self.is_prefixed:
                        return (
                            SCAN_OP_PREFIX,
                            ind + 1,  # cover key length
                            1,  # priority
                            lambda k: self.search_by_key_prefix(k + type_format_mappers[type(c_eq)](c_eq)),
                            _q_arr_eq
                        )

                    return SCAN_OP_SEQ, 0, 0, None, None

                c_in = condition.get('$in', EMPTY)

                if c_in != EMPTY:
                    if ind == self.schema_last_index:
                        return (
                            SCAN_OP_IN,
                            ind + 1,  # cover key length
                            2,  # priority
                            lambda k: self.search_by_key_in(
                                map(lambda x: k + type_format_mappers[type(x)](x), c_in)
                            ),
                            _q_arr_in
                        )

                    if self.is_prefixed:
                        return (
                            SCAN_OP_PREFIX_IN,
                            ind + 1,  # cover key length
                            1,  # priority
                            lambda k: self.search_by_key_prefix_in(
                                map(lambda x: k + type_format_mappers[type(x)](x), c_in)
                            ),
                            _q_arr_in
                        )

                    return SCAN_OP_SEQ, 0, 0, None, None

                if self.is_ranged:
                    c_gt = condition.get('$gt', EMPTY)
                    c_gte = condition.get('$gte', c_gt)
                    c_lt = condition.get('$lt', EMPTY)
                    c_lte = condition.get('$lte', c_lt)

                    if c_gt != EMPTY or c_gte != EMPTY or c_lt != EMPTY or c_lte != EMPTY:
                        if c_gte != EMPTY:
                            c_gte = type_format_mappers[type(c_gte)](c_gte)

                        if c_lte != EMPTY:
                            c_lte = type_format_mappers[type(c_lte)](c_lte)

                        return (
                            SCAN_OP_RANGE,
                            ind + 1,  # cover key length
                            1,  # priority
                            lambda k: self.search_by_key_range(
                                (k + c_gte) if c_gte != EMPTY else k + chr(0),
                                (k + c_lte) if c_lte != EMPTY else k + CHAR255,
                                c_gt != EMPTY,
                                c_lt != EMPTY,
                            ),
                            _q_arr_range,
                        )

                if self.is_prefixed:
                    return SCAN_OP_PREFIX, ind, 1, self.search_by_key_prefix, None

                return SCAN_OP_SEQ, 0, 0, None, None

        return SCAN_OP_CONST, ind + 1, 2, self.search_by_key, None

    def set_float_precision(self, precision=18):
        self.type_format_mappers = configure_float_precision(precision)

        return self

    def append_key(self, key, default_value=EMPTY):
        self.schema[key] = default_value
        self.schema_keys.append(key)
        self.schema_last_index += 1

        return self

    def clear(self):
        return self

    def delete(self, key, uid):
        raise NotImplementedError

    def insert(self, key, uid):
        raise NotImplementedError

    def insert_by_schema(self, values, uid):
        if self.schema_default_values:
            second = {}

            for key, val in self.schema_default_values.items():
                if key not in values:
                    if callable(val):
                        second[key] = val(key, values)
                    else:
                        second[key] = val
        else:
            second = None

        self.insert(self.get_cover_key_or_raise(values, second), uid)

        return True

    def insert_is_allowed(self, key):
        return True

    def search_by_key(self, key):
        raise NotImplementedError

    def search_by_key_in(self, keys):
        raise NotImplementedError

    def search_by_key_prefix(self, key):
        raise NotImplementedError

    def search_by_key_prefix_in(self, keys):
        raise NotImplementedError

    def search_by_key_range(self, gte=None, lte=None, gte_excluded=True, lte_excluded=True):
        raise NotImplementedError

    def search_by_key_seq(self, q, source):
        raise NotImplementedError

    def upsert(self, old, new, uid):
        raise NotImplementedError

    def upsert_is_allowed(self, old, new):
        return True


class UdbEmbeddedIndex(UdbIndex):
    is_embedded = True

    @classmethod
    def seq(cls, seq, q, collection):
        for rid in seq:
            passed = True
            record = collection[rid]

            for key, condition in q.items():
                _ = record.get(key, None)

                if condition and type(condition) == dict:
                    for op_key, op_condition in condition.items():
                        op = cls._OPS.get(op_key)

                        if op:
                            passed = op(_, op_condition)

                        if not passed:
                            break
                else:
                    passed = _eq_op(_, condition)

                if not passed:
                    break

            if passed:
                yield rid

    def get_cover_key(self, record, second=None):
        key = ''
        type_format_mappers = self.type_format_mappers

        for i, k in enumerate(self.schema_keys):
            get = self.schema[k]

            if callable(get):
                val = get(k, record)
            elif second:
                val = second.get(k, get)

                if val == EMPTY:
                    val = record.get(k, get)
            else:
                val = record.get(k, get)

            if val != EMPTY:
                if i == self.schema_last_index:
                    if type(val) in self.type_format_mappers:
                        yield key + type_format_mappers[type(val)](val)
                    else:
                        for val in val:
                            yield key + type_format_mappers[type(val)](val)
                else:
                    key += type_format_mappers[type(val)](val)
            else:
                break
