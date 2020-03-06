import collections

from ..common import (
    FieldRequiredError,
    InvalidScanOperationValueError,
    CHAR255,
    EMPTY,
    TYPE_COMPARERS,
)
from ..udb_index import UdbIndex, SCAN_OP_CONST, SCAN_OP_SEQ


def _q_arr_eq(q):
    q.pop('$eq')


def _q_arr_in(q):
    q.pop('$in')


def _q_arr_range(q):
    q.pop('$gt', q.pop('$gte', q.pop('$lt', q.pop('$lte', EMPTY))))


_PRIMITIVE_VALS = (None, bool, float, int, str)


SCAN_OP_EMPTY = 'empty'
SCAN_OP_IN = 'in'
SCAN_OP_PREFIX = 'prefix'
SCAN_OP_PREFIX_IN = 'prefix_in'
SCAN_OP_RANGE = 'range'


def _eq_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a is b


def _gt_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a > b if can_be_compared is None else can_be_compared


def _gte_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a >= b if can_be_compared is None else can_be_compared


def _in_op(a, b):
    for v in b:
        if a is v:
            return True

    return False  # return a in b


def _lt_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a < b if can_be_compared is None else not can_be_compared


def _lte_op(a, b):
    can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a <= b if can_be_compared is None else not can_be_compared


def _ne_op(a, b):
    # can_be_compared = TYPE_COMPARERS[type(a)][type(b)]

    return a is not b


def _nin_op(a, b):
    for v in b:
        if a is v:
            return False

    return True  # return a not in b


class UdbBaseLinearIndex(UdbIndex):
    is_embedded = False
    is_multivalued = False
    is_prefixed = False
    is_ranged = False
    is_sorted_asc = False
    is_sparse = False
    is_uniq = False

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
    def check_condition(cls, values, q, context=None, extend=None):
        for key, condition in q.items():
            val = values.get(key, EMPTY)

            if val is EMPTY and extend:
                val = extend.get(key, EMPTY)

            is_acceptable = val is not EMPTY

            if condition and type(condition) == dict:
                for op_key, op_condition in condition.items():
                    op = cls._OPS.get(op_key)

                    if op and (not is_acceptable or not op(val, op_condition)):
                        return False
            else:
                if not is_acceptable or not _eq_op(val, condition):
                    return False

        return True

    @classmethod
    def seq(cls, seq, q, collection):
        for rid in seq:
            if cls.check_condition(collection[rid], q):
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

    def __init__(self, schema, name=None, sparse=False):
        """
        :param schema: Index schema as list of keys or set of keys and accessors.
        :param name:
        :param sparse:
        """
        UdbIndex.__init__(self, name)

        self.is_sparse = sparse

        if type(schema) == list or type(schema) == tuple:
            schema = collections.OrderedDict(
                (v[0], v[1])
                if type(v) == list or type(v) == tuple
                else (v, EMPTY) for v in schema
            )

        self.schema = schema
        self.schema_default_values = {key: val for key, val in schema.items() if val != EMPTY}
        self.schema_keys = list(schema.keys())
        self.schema_last_index = len(schema) - 1

    def get_key(self, key, default=None):
        raise NotImplementedError

    def get_cover_key(self, record, second=None):
        cover_key = ''

        for ind, key in enumerate(self.schema_keys):
            get = self.schema[key]

            if callable(get):
                val = get(key, record)
            elif second:
                val = second.get(key, get)

                if val == EMPTY:
                    val = record.get(key, get)
            else:
                val = record.get(key, get)

            if val != EMPTY:
                cover_key += self.type_format_mappers[type(val)](val)
            else:
                if self.is_sparse:
                    if ind > 0:
                        cover_key += self.type_format_mappers[type(None)](None)
                    else:
                        return None
                else:
                    return None

        return cover_key

    def get_cover_key_or_raise(self, record, second=None):
        cover_key = ''

        for ind, key in enumerate(self.schema_keys):
            get = self.schema[key]

            if callable(get):
                val = get(key, record)
            elif second:
                val = second.get(key, get)

                if val == EMPTY:
                    val = record.get(key, get)
            else:
                val = record.get(key, get)

            if val != EMPTY:
                cover_key += self.type_format_mappers[type(val)](val)
            else:
                if self.is_sparse:
                    if ind > 0:
                        cover_key += self.type_format_mappers[type(None)](None)
                    else:
                        return None
                else:
                    raise FieldRequiredError('field required: {} on {}'.format(key, self.name))

        return cover_key

    def get_meta(self):
        return {'schema': self.schema, 'name': self.name, 'sparse': self.is_sparse}

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
        ind = -1

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


class UdbBaseLinearEmbeddedIndex(UdbBaseLinearIndex):
    is_embedded = True

    @classmethod
    def seq(cls, seq, q, collection):
        """
        Sequential scan

        :param seq: Sequence of record ids (rid)
        :param q: Query
        :param collection: Collection of records Dict[Str (rid), Dict[Str, Any]]

        :return:
        """
        for rid in seq:
            passed = True
            record = collection[rid]

            for key, condition in q.items():
                record_value = record.get(key, EMPTY)

                if condition and type(condition) == dict:
                    for op_key, op_condition in condition.items():
                        op = cls._OPS.get(op_key)

                        if op:
                            passed = op(record_value, op_condition)

                            if not passed:
                                break
                else:
                    passed = _eq_op(record_value, condition)

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
