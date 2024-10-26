from ..common import EMPTY, FieldRequiredError, InvalidScanOperationValueError
from ..udb_index import UdbIndex, SCAN_OP_SEQ


def _q_arr_text(q):
    q.pop('$text')


SCAN_OP_TEXT = 'text'


class UdbBaseTextIndex(UdbIndex):
    is_embedded = False
    is_multivalued = False
    is_prefixed = False
    is_ranged = False
    is_sorted_asc = False
    is_sparse = False
    is_uniq = False

    @classmethod
    def check_condition(cls, values, q, context=None, extend=None):
        return True

    @classmethod
    def seq(cls, seq, q, collection):
        has_condition = False

        for cnd in q.values():
            if type(cnd) == dict and '$text' in cnd:
                has_condition = True
                break

        if not has_condition:
            return seq

        def gen():
            for rid in seq:
                if cls.check_condition(collection[rid], q):
                    yield rid

        return gen()

    def get_cover_key(self, record, second=None):
        cover_key = {}

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

            if val == EMPTY:
                pass
                # if ind == 0:
                #     return None
                # else:
                #     cover_key[key] = None
            else:
                cover_key[key] = val

        return cover_key

    def get_cover_key_or_raise(self, record, second=None):
        cover_key = {}

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

            if val == EMPTY:
                if ind == 0:
                    return None
                else:
                    raise FieldRequiredError('field required: {} on {}'.format(key, self.name))
            else:
                cover_key[key] = val

        return cover_key

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
        schema_keys_matched = None

        for ind, key in enumerate(self.schema_keys):
            condition = q.get(key, EMPTY)

            if type(condition) == dict:
                c_text = condition.get('$text')

                if c_text:
                    if schema_keys_matched is None:
                        schema_keys_matched = {}

                    schema_keys_matched[key] = c_text

        if not schema_keys_matched:
            return SCAN_OP_SEQ, 0, 0, 0, None, None

        return (
            SCAN_OP_TEXT,
            len(schema_keys_matched),
            len(schema_keys_matched),
            3,
            lambda _: self.search_by_text(schema_keys_matched),
            _q_arr_text,
        )

    def search_by_text(self, q):
        raise NotImplementedError
