import collections

from ..common import EMPTY
from .udb_base_text_index import UdbBaseTextIndex


class UdbTextIndex(UdbBaseTextIndex):
    type = 'text'

    def __init__(self, schema, name=None):
        from whoosh.fields import ID, NUMERIC, Schema, TEXT
        from whoosh.filedb.filestore import RamStorage
        from whoosh.qparser import QueryParser

        UdbBaseTextIndex.__init__(self, name)

        if type(schema) == list or type(schema) == tuple:
              schema = collections.OrderedDict(
                (v[0], v[1])
                if type(v) == list or type(v) == tuple
                else (v, EMPTY) for v in schema
            )

        schema = {
            k: EMPTY if type(v) == dict and '__emp__' in v else v
            for k, v in schema.items()
        }

        self.schema = schema
        self.schema_default_values = {key: val for key, val in schema.items() if val != EMPTY}
        self.schema_keys = list(schema.keys())
        self.schema_last_index = len(schema) - 1

        self._whoosh_schema = Schema(**{key: TEXT(stored=True) for key in self.schema_keys}, udb__uid__=NUMERIC(stored=True))
        self._whoosh_storage = RamStorage()
        self._whoosh_index = self._whoosh_storage.create_index(self._whoosh_schema)
        self._whoosh_parser = QueryParser('a', schema=self._whoosh_index.schema)
        self._whoosh_writer = None

    def __len__(self):
        return 0

    def clear(self):
        #self._btree.clear()

        return self

    def delete(self, key_dict, uid=None):
        #self._btree.pop(key_or_keys, EMPTY)

        return self

    def insert(self, key_dict, uid):
        if key_dict:
            if self._whoosh_writer is None:
                self._whoosh_writer = self._whoosh_index.writer()

            self._whoosh_writer.add_document(**key_dict, udb__uid__=uid)

        return self

    def search_by_text(self, q):
        if self._whoosh_writer:
          self._whoosh_writer.commit()
          self._whoosh_writer = None

        with self._whoosh_index.searcher() as searcher:
            for rec in searcher.search(self._whoosh_parser.parse(' '.join(q.values())), limit=None):
                yield rec['udb__uid__']

    def upsert(self, old, new, uid):
        # if old != new:
        #     self._btree.pop(old)

        # self._btree.insert(new, uid)

        return self
