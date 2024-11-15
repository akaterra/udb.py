import collections

from ..common import EMPTY
from .udb_base_text_index import UdbBaseTextIndex


WRITER_OPENED_BY_DELETE = 0
WRITER_OPENED_BY_INSERT = 1
WRITER_OPENED_BY_UPSERT = 2


class UdbTextIndex(UdbBaseTextIndex):
    type = 'text'

    def __init__(self, schema, name=None):
        from whoosh.fields import ID, NUMERIC, Schema, STORED, TEXT
        from whoosh.filedb.filestore import RamStorage
        from whoosh.qparser import QueryParser
        from whoosh import query

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

        self._whoosh_schema = Schema(udb__uid__=ID(unique=True, stored=True), **{key: TEXT(phrase=False) for key in self.schema_keys})
        self._whoosh_storage = RamStorage()
        self._whoosh_index = self._whoosh_storage.create_index(self._whoosh_schema)
        self._whoosh_parser = QueryParser('a', schema=self._whoosh_index.schema)
        self._whoosh_query = query
        self._whoosh_writer = None
        self._whoosh_writer_opened_by = None

    def __len__(self):
        return 0

    def clear(self):
        return self._whoosh_index.doc_count

    def delete(self, key_dict, uid=None):
        if key_dict:
            if self._whoosh_writer_opened_by != WRITER_OPENED_BY_DELETE:
                if self._whoosh_writer:
                    self._whoosh_writer.commit(optimize=True)

                self._whoosh_writer = self._whoosh_index.writer()
                self._whoosh_writer_opened_by = WRITER_OPENED_BY_DELETE

            self._whoosh_writer.delete_by_term('udb__uid__', str(uid))

        return self

    def insert(self, key_dict, uid):
        if key_dict:
            if self._whoosh_writer_opened_by != WRITER_OPENED_BY_INSERT:
                if self._whoosh_writer:
                    self._whoosh_writer.commit(optimize=True)

                self._whoosh_writer = self._whoosh_index.writer()
                self._whoosh_writer_opened_by = WRITER_OPENED_BY_INSERT

            self._whoosh_writer.add_document(udb__uid__=str(uid), **key_dict)

        return self

    def search_by_text(self, q):
        if self._whoosh_writer:
            self._whoosh_writer.commit(optimize=True)
            self._whoosh_writer = None
            self._whoosh_writer_opened_by = None

        with self._whoosh_index.searcher() as searcher:
            for rec in searcher.search(self._whoosh_parser.parse(' '.join(q.values())), limit=1):
                yield int(rec['udb__uid__'])

    def upsert(self, old_dict, new_dict, uid):
        if new_dict:
            if self._whoosh_writer_opened_by != WRITER_OPENED_BY_UPSERT:
                if self._whoosh_writer:
                    self._whoosh_writer.commit(optimize=True)

                self._whoosh_writer = self._whoosh_index.writer()
                self._whoosh_writer_opened_by = WRITER_OPENED_BY_UPSERT

            self._whoosh_writer.update_document(udb__uid__=str(uid), **new_dict)

        return self
