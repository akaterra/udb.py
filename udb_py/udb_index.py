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
    is_custom_ops = True
    is_multivalued = False
    is_sorted_asc = False
    is_uniq = False
    schema = {}
    schema_default_values = None
    schema_keys = []
    schema_last_index = -1
    type = None
    type_format_mappers = TYPE_FORMAT_MAPPERS

    _name = 'index'
    _safe = False

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

    @property
    def name(self):
        return self._name or type(self).__name__

    def __init__(self, name=None):
        self._name = name

    def get_cover_key(self, record, second=None):
        raise NotImplementedError

    def get_cover_key_or_raise(self, record, second=None):
        raise NotImplementedError

    def get_indexes_with_custom_ops(self):
        return [self]

    def get_meta(self):
        raise NotImplementedError

    def get_scan_op(self, q, limit=None, offset=None, collection=None, indexes_with_custom_ops=None):
        raise NotImplementedError

    def get_scan_op_cover_key(
            self,
            q,
            key_sequence_length_to_remove,
            fn_q_arranger,
    ):
        key = ''
        i = -1

        for i in range(0, key_sequence_length_to_remove):
            key_val = q.pop(self.schema_keys[i])
            key += self.type_format_mappers[type(key_val)](key_val)

        if fn_q_arranger:
            key_next = self.schema_keys[i + 1]
            fn_q_arranger(q[key_next])

            # query key becomes empty after arranger like {} after {'$gt': ...}
            if not q[key_next]:
                q.pop(key_next)

        return key

    def set_float_precision(self, precision=18):
        self.type_format_mappers = configure_float_precision(precision)

        return self

    def set_name_if_default(self, name):
        if not self._name:
            self._name = name

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

    def clone(self):
        raise NotImplementedError
    
    def rids(self):
        raise NotImplementedError

    def safe(self, safe=True):
        self._safe = safe

        return self

    def delete(self, key, uid, q=None):
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

        key, key_len = self.get_cover_key(values, second)

        if key_len:
            self.insert(key, uid)

        return True

    def insert_is_allowed(self, key):
        return True

    def upsert(self, old, new, uid, q=None):
        raise NotImplementedError

    def upsert_is_allowed(self, old, new, q=None):
        return True
