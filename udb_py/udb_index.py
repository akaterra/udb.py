from .common import (
    EMPTY,
    TYPE_FORMAT_MAPPERS,
    configure_float_precision,
)


_PRIMITIVE_VALS = (None, bool, float, int, str)


SCAN_OP_CONST = 'const'
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
    def check_condition(cls, values, q, context=None, extend=None):
        raise NotImplementedError

    @classmethod
    def create_condition_context(cls, q):
        return None

    @classmethod
    def merge_condition(cls, q1, q2, context=None, extend=None):
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
        raise NotImplementedError

    def set_float_precision(self, precision=18):
        self.type_format_mappers = configure_float_precision(precision)

        return self
    
    def has_key(self, key):
        return key in self.schema

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
