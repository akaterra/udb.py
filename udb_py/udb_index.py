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


_PRIMITIVE_VALS = (None, bool, float, int, str)


SCAN_OP_CONST = 'const'
# SCAN_OP_EMPTY = 'empty'
# SCAN_OP_IN = 'in'
# SCAN_OP_INTERSECTION = 'intersection'
# SCAN_OP_NEAR = 'near'
# SCAN_OP_PREFIX = 'prefix'
# SCAN_OP_PREFIX_IN = 'prefix_in'
# SCAN_OP_RANGE = 'range'
SCAN_OP_SEQ = 'seq'
SCAN_OP_SORT = 'sort'
SCAN_OP_SUB = 'sub'


class UdbIndex(object):
    is_sorted_asc = False
    is_uniq = False
    name = 'index'
    schema = {}
    schema_default_values = None
    schema_keys = []
    schema_last_index = - 1
    type = None
    type_format_mappers = TYPE_FORMAT_MAPPERS

    @classmethod
    def check_condition(cls, values, q, context=None):
        raise NotImplementedError

    @classmethod
    def seq(cls, seq, q, collection):
        raise NotImplementedError

    @classmethod
    def validate_query(cls, q):
        raise NotImplementedError

    def __init__(self, name=None):
        self.name = name or type(self).__name__

    def get_cover_key(self, record, second=None):
        raise NotImplementedError

    def get_cover_key_or_raise(self, record, second=None):
        raise NotImplementedError

    def get_meta(self):
        return {}

    def set_float_precision(self, precision=18):
        self.type_format_mappers = configure_float_precision(precision)

        return self

    def append_key(self, key, default_value=EMPTY):
        self.schema[key] = default_value
        self.schema_keys.append(key)
        self.schema_last_index += 1

        return self

    def clear(self):
        raise NotImplementedError

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
