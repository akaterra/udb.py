import pytest

from udb_py.common import *
from udb_py.index.udb_text_index import UdbTextIndex


class UdbTextIndexTest(UdbTextIndex):
    @property
    def index(self):
        if self._whoosh_writer:
            self._whoosh_writer.commit()
            self._whoosh_writer = None

        return self._whoosh_index

    @property
    def parser(self):
        return self._whoosh_parser

# def test_should_delete():
#     i = UdbBtreeIndexTest(['a', 'b', 'c'])

#     i.insert('123', 123).delete('123')

#     assert i.index.get('123', 0) == 0


def test_should_insert():
    i = UdbTextIndexTest(['a'])

    i.insert({'a': '123'}, 123)

    assert list(i.index.searcher().search(i.parser.parse('123'))) == [{'udb__uid__': str(123)}]


def test_should_insert_by_schema():
    i = UdbTextIndexTest(['a'])

    i.insert_by_schema({'a': '123'}, 123)

    assert list(i.index.searcher().search(i.parser.parse('123'))) == [{'udb__uid__': str(123)}]


def test_should_insert_by_schema_with_default_value():
    i = UdbTextIndexTest({'a': '123'})

    i.insert_by_schema({'c': 1}, 123)

    assert list(i.index.searcher().search(i.parser.parse('123'))) == [{'udb__uid__': str(123)}]


def test_should_insert_by_schema_with_default_value_as_callable():
    i = UdbTextIndexTest({'a': lambda key, values: '123'})

    i.insert_by_schema({'c': 1}, 123)

    assert list(i.index.searcher().search(i.parser.parse('123'))) == [{'udb__uid__': str(123)}]


def test_should_upsert():
    i = UdbTextIndexTest(['a'])

    i.insert({'a': '123'}, 123).upsert({'a': '123'}, {'a': '321'}, 123)

    assert list(i.index.searcher().search(i.parser.parse('321'))) == [{'udb__uid__': str(123)}]


def test_should_upsert_deleting_old_key():
    i = UdbTextIndexTest(['a'])

    i.insert({'a': '123'}, 123).upsert({'a': '123'}, {'a': '321'}, 123)

    assert list(i.index.searcher().search(i.parser.parse('123'))) == []


# def test_should_search_by_key():
#     i = UdbBtreeIndexTest(['a', 'b', 'c'])

#     i.insert('123', 123).insert('321', 321).insert('111', 111).insert('333', 333)

#     assert list(i.search_by_key('123')) == [123]
