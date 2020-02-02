Udb.py
======

.. image:: https://travis-ci.org/akaterra/udb.py.svg?branch=master
  :target: https://travis-ci.org/akaterra/udb.py

Udb is an in-memory database based on the `Zope Foundation BTrees <https://github.com/zopefoundation/BTrees>`_, the `Rtree <https://rtree.readthedocs.io/en/latest>`_ and on the native python's dict.
Udb provides indexes support and limited MongoDB-like queries.
Udb does not support any type of transactions for now.

Table of contents
-----------------

* `Requirements <#requirements>`_

* `Installation <#installation>`_

* `Quick start <#quick-start>`_

* `Data schema for default values <#data-schema-for-default-values>`_

  * `Functional fields <#functional-fields>`_

* `Indexes <#indexes>`_

  * `Index declaration <#index-declaration>`_

  * `Float precision <#float-precision>`_

* `Querying <#querying>`_

  * `Query validation <#query-validation>`_

  * `Comparison order <#comparison-order>`_

  * `Getting plan <#getting-plan>`_

  * `Scan operations <#scan-operations>`_

* `Storages <#storages>`_

* `Select operation <#select-operation>`_

* `Delete operation <#delete-operation>`_

* `Insert operation <#insert-operation>`_

* `Update operation <#update-operation>`_

* `Instant view <#instant-view>`_

* `Limitations <#limitations>`_

* `Running tests <#running-tests-with-pytest>`_

* `Benchmarks <#benchmarks>`_

Requirements
------------

Python 2.7, Python 3.6

Installation
------------

.. code:: bash

  pip install udb_py

To enable BTree indexes support install `Zope Foundation BTrees <https://github.com/zopefoundation/BTrees>`_ package:

.. code:: bash

  pip install BTrees

To enable RTree indexes support install `Rtree <http://toblerity.org/rtree>`_ package (requires `libspatialindex <https://libspatialindex.org>`_):

.. code:: bash

  pip install Rtree

  Installing **libspatialindex** on MacOS with Homebrew:

.. code:: bash

  brew install spatialindex

Quick start
-----------

Create the Udb instance with the indexes declaration:

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb({
      'a': UdbBtreeIndex(['a']),
      'b': UdbBtreeIndex(['b']),
      'cde': UdbBtreeIndex(['c', 'd', 'e']),
  })

Insert records:

.. code:: python

  db.insert({'a': 1, 'b': 1, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 2, 'b': 2, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 3, 'b': 3, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 4, 'b': 4, 'c': 3, 'd': 4, 'e': 6})
  db.insert({'a': 5, 'b': 5, 'c': 3, 'd': 4, 'e': 7})

Select records:

.. code:: python

  a = list(db.select({'a': 1})

  [{'a': 1, 'b': 1, 'c': 3, 'd': 4, 'e': 5}]

  b = list(db.select({'b': 0})

  []  # no records with b=0

  c = list(db.select({'c': 3, 'd': 4}, limit=2)

  [{'a': 3, 'b': 3, 'c': 3, 'd': 4, 'e': 5}, {'a': 4, 'b': 4, 'c': 3, 'd': 4, 'e': 6}]

Data schema for default values
------------------------------

Data schema allows to fill the inserted record with default values.
The default value can be defined as a primitive value or callable:

.. code:: python

  from udb_py import Udb

  db = Udb(schema={
      'a': 'a',
      'b': 'b',
      'c': lambda key, record: 'b' if record['b'] == 'b' else 'c',
  })

Functional fields
~~~~~~~~~~~~~~~~~

**auto_id** - generates unique id (uuid v1 by default)

.. code:: python

  from udb_py import Udb, auto_id

  db = Udb(schema={
      'id': auto_id(),
  })

**current_timestamp** - uses current timestamp (as int value)

.. code:: python

  from udb_py import Udb, current_timestamp

  db = Udb(schema={
      'timestamp': current_timestamp(),
  })

**fn** - calls custom function

.. code:: python

  from udb_py import Udb, fn

  db = Udb(schema={
      'timestamp': fn(lambda record: record['a'] + record['b']),
  })

**optional** - returns "None" value

.. code:: python

  from udb_py import Udb, optional

  db = Udb(schema={
      'a': optional,
  })

Indexes
-------

To speed up the search for records, the necessary fields can be indexed.
The Udb also includes a simple query optimiser that can select the most appropriate index.

BTree indexes:

* **UdbBtreeMultivaluedIndex** - btree based multivalued index supporting multiple records with the same index key.

* **UdbBtreeMultivaluedEmbeddedIndex** - same as the **UdbBtreeMultivaluedIndex**, but supports embedded list of values.

* **UdbBtreeUniqIndex** - btree based index operating with always single records, but the second record insertion with the same index key will raise IndexConstraintError.

* **UdbBtreeIndex** - btree based index operating with always single records, so that the second record insertion with the same index key will overwrite the old one. Can be used when the inserting record definitely generates a unique index key.

Hash indexes:

* **UdbHashMultivaluedIndex** - hash based multivalued index supporting multiple records with the same index key.

* **UdbHashMultivaluedEmbeddedIndex** - same as the **UdbHashMultivaluedIndex**, but supports embedded list of values.

* **UdbHashUniqIndex** - hash based index operating with always single records, but the second record insertion with the same index key will raise IndexConstraintError.

* **UdbHashIndex** - hash based index operating with always single records, so that the second record insertion with the same index key will overwrite the old one. Can be used when the inserting record definitely generates a unique index key.

Spatial indexes:

* **UdbRtreeIndex** - spatial index that supports "intersection with rectangle" and "near to point" search

Index declaration
~~~~~~~~~~~~~~~~~

As it was shown `above <#quick-start>`_, for the index declaration the Udb instance should be created with the **indexes** parameter that provides dict with the key as an index name and value as an index instance.
The index instance should be created with the sequence of fields (1 at least) which will be fetched in the declared order from the indexed record.
By this sequence of fields, the index key will be generated and will be associated with the indexed record.

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex(['a', 'b', 'c'])  # "a", "b" and "c" fields will be fetched from the indexed record
  })

  record = {'a': 'A', 'b': 'B', 'c': 'C'}  # index key=ABC

In this case of declaration in order that the record to be indexed it must contain all of the fields declared in the sequence of index fields.

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex(['a', 'b', 'c'])  # "a", "b" and "c" fields will be fetched from the indexed record
  })

  record = {'a': 'A', 'b': 'B'}  # won't be indexed, raises FieldRequiredError

Using dictionary in case of Python 3:

.. code:: python

  from udb_py import Udb, UdbBtreeIndex, required

  db = Udb(indexes={
      'abc': UdbBtreeIndex({'a': required, 'b': required, 'c': required})  # "a", "b" and "c" fields will be fetched from the indexed record
  })

  record = {'a': 'A', 'b': 'B'}  # won't be indexed, raises FieldRequiredError

Using list of tuples in case of Python 2 (to keep key order):

.. code:: python

  from udb_py import Udb, UdbBtreeIndex, required

  db = Udb(indexes={
      'abc': UdbBtreeIndex([('a', required), ('b', required), ('c', required)])  # "a", "b" and "c" fields will be fetched from the indexed record
  })

  record = {'a': 'A', 'b': 'B'}  # won't be indexed, raises FieldRequiredError

The default value for missing field can be defined as a primitive value or callable (functional index):

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex({'a': 'a', 'b': 'b', 'c': 'c'})
  })

  record = {'a': 'A', 'c': 'C'}  # index key=AbC

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex({'a': 'a', 'b': lambda key, values: 'b', 'c': 'c'})
  })

  record = {'a': 'A', 'c': 'C'}  # index key=AbC

Float precision
~~~~~~~~~~~~~~~

To be able to index float values enable the float mode with necessary precision (number of decimals):

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex(['a']).set_float_precision(10)
  })

  db.insert({'a': 3.1415926525})

Querying
--------

Udb supports limited MongoDB-like queries which can be used in the delete, select or update operations.
The query generally is a python's dict with the key as a field and value as a primitive value or an equality condition over the field.
The query dict is **mutable**, therefore it needs to be initialized every time anew.

Supported query operations:

* **$eq** - equal to a value

  .. code:: python

    udb.select({'a': {'$eq': 5}})

* **$gt** - greater then value

  .. code:: python

    udb.select({'a': {'$gt': 5}})

* **$gte** - greater or equal to a value

  .. code:: python

    udb.select({'a': {'$gte': 5}})

* **$in** - equal to an any value in the list of a values

  .. code:: python

    udb.select({'a': {'$in': 5}})

* **$intersection** - intersection with rectangle

  .. code:: python

    udb.select({'a': {'$intersection': {'minX': 5, 'minY': 5, 'maxX': 1, 'maxY': 5}}})

* **$lt** - less then value

  .. code:: python

    udb.select({'a': {'$lt': 5}})

* **$lte** - less or equal to a value

  .. code:: python

    udb.select({'a': {'$lte': 5}})

* **$ne** - not equal to a value

  .. code:: python

    udb.select({'a': {'$ne': 5}})

  * performs "seq" scan.

* **$near** - near to point with optional min and max distances

  .. code:: python

    udb.select({'a': {'$near': {'x': 5, 'y': 5, 'minDistance': 1, 'maxDistance': 5}}})

  * allocates sort buffer is case of "seq" scan

  * selects all records in case of unset *maxDistance* and set *minDistance*.

* **$nin** - not equal to an any value in the list of a values

  .. code:: python

    udb.select({'a': {'$nin': [1, 2, 3]}})

  * performs "seq" scan.

* **primitive value** - equal to a value

  .. code:: python

    udb.select({'a': 5})

Example:

.. code:: python

  records = list(udb.select({'a': 1}))
  records = list(udb.select({'a': {'$gte': 1, '$lte': 3}}))
  records = list(udb.select({'a': {'$in': [1, 2, 3], '$lte': 2}}))

Query validation
~~~~~~~~~~~~~~~~

By default Udb does not check the query dict validity.
To check its validity use **validate_query** method.

.. code:: python

  udb.validate_query({'a': {'$gte': [1, 2, 3]}})  # raises InvalidScanOperationValueError('a.$gte')

Comparison order
~~~~~~~~~~~~~~~~

Due to the fact that the Udb database is not strictly typed for stored values, there is the following order of ascending comparisons for values ​​of different types:

* None

* boolean - *false* less then *true*

* int, float

* string

So, for example, the record containing *int* value always greater than the record containing *boolean* value for the same field.
Also, it means, that the records having indexed field will be fetched in the provided order.

Getting plan
~~~~~~~~~~~~

To get the query plan use **select** method with **get_plan=True**:

.. code:: python

  from udb_py import Udb, UdbBtreeIndex

  db = Udb(indexes={
      'abc': UdbBtreeIndex({'a': 'a', 'b': lambda key, values: 'b', 'c': 'c'})
  })

  db.select({'a': 3}, sort='-a', get_plan=True)  # [(<udb.index.udb_btree_index.UdbBtreeIndex object at 0x104994080>, 'const', 1, 2), (None, 'sort', 0, 0, 'a', False)]

Scan operations
~~~~~~~~~~~~~~~

BTree index:

* **const** - an index covers only one record by the index key

* **in** - an index covers multiple records by the list of the index keys, each of which covers exactly one record

* **range** - an index covers multiple records by the index keys set by the minimum and maximum values

* **prefix** - an index covers range of records by the partial index key

* **prefix_in** - an index covers multiple records by the list of the partial index keys, each of which covers range of records

RTree index:

* **intersection** - an index covers records intersected by the rectangle

* **near** - an index covers records near to the point

No index:

* **seq** - scanning that is not covered by any index, all records will be scanned (worst case)

Storages
--------

The storage allows keeping data persistent.

**UdbJsonFileStorage** stores data in the JSON file.

.. code:: python

  from udb_py import UdbJsonFileStorage

  db = Udb(storage=UdbJsonFileStorage('db'))

  db.load_db()

  db.insert({'a': 'a'})

  db.save_db()

**UdbWalStorage** stores data of delete, insert and update operations in the WAL (Write-Ahead-Logging) file chronologically.

.. code:: python

  from udb_py import UdbWalStorage

  db = Udb(storage=UdbWalStorage('db'))

  db.load_db()

  db.insert({'a': 'a'})

  db.save_db()  # does nothing; delete, insert and update data will be stored on the fly

Select operation
----------------

Selected records are **mutable**, so avoid to update them directly.
Otherwise use copy on select mode:

.. code:: python

  udb.set_copy_on_select()

To limit the result subset to particular number of records use **limit** parameter:

.. code:: python

  records = list(udb.select({'a': 1}, limit=5)

To fetch the result subset from the particular offset use **offset** parameter:

.. code:: python

  records = list(udb.select({'a': 1}, offset=5)

Delete operation
----------------

.. code:: python

  udb.delete(q={'a': 1}, offset=5)

Insert operation
----------------

.. code:: python

  udb.insert({'a': 1})

Update operation
----------------

.. code:: python

  udb.update({'a': 2}, q={'a': 1}, offset=5)

Running tests with pytest
-------------------------

.. code:: bash

  pytest . --ignore=virtualenv -v

Instant view
------------

Instant view allows to get an instant slice of record by condition.

.. code:: python

  from udb_py import Udb, UdbView

  db = Udb({})

  db = Udb({
      'a': UdbBtreeIndex(['a']),
      'b': UdbBtreeIndex(['b']),
      'cde': UdbBtreeIndex(['c', 'd', 'e']),
  })

  db.insert({'a': 1, 'b': 1, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 2, 'b': 2, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 3, 'b': 3, 'c': 3, 'd': 4, 'e': 5})
  db.insert({'a': 4, 'b': 4, 'c': 3, 'd': 4, 'e': 6})
  db.insert({'a': 5, 'b': 5, 'c': 3, 'd': 4, 'e': 7})

  view = UdbView(db, {'b': {'$gte': 3}})

  db.insert({'a': 6, 'b': 6, 'c': 3, 'd': 4, 'e': 8})  # updates view immediately

  view.select({'a': 6})  # {'a': 5, 'b': 5, 'c': 3, 'd': 4, 'e': 7}

By default view has the same indexes as the provided Udb instance.
Use **indexes** parameter to drop all indexes or to set your own.

.. code:: python

  view = UdbView(db, {'b': {'$gte': 3}}, indexes=None)  # view has no indexes

.. code:: python

  view = UdbView(db, {'b': {'$gte': 3}}, indexes={'a': UdbBtreeIndex(['a'])})  # view has custom indexes

Limitations
-----------

* Nested paths for indexing and querying are not supported, only the root level

* Transactions are not supported

Benchmarks
----------

* Intel Core i7, 3.58 GHz, 4 cores, disabled HT

* 16GB 1600 MHz RAM

* PyPy3

.. code:: text

  INSERT (BTREE, 1ST INDEX COVERS 1 FIELD)

  Total time: 2.9712460041046143 sec., per sample: 2.971246004104614e-06 sec., samples per second: 336559.1400437912, total samples: 1000000

  SELECT (BTREE, 1ST INDEX COVERS 1 FIELD)

  Total time: 1.7301840782165527 sec., per sample: 1.7301840782165527e-06 sec., samples per second: 577973.1836573046, total samples: 1000000

  INSERT (BTREE, 1ST INDEX COVERS 1 FIELD, 2ND INDEX COVERS 1 FIELD, 3RD INDEX COVERS 2 FIELDS)

  Total time: 6.8810200691223145 sec., per sample: 6.881020069122315e-06 sec., samples per second: 145327.29013353275, total samples: 1000000

  SELECT (BTREE, 1ST INDEX COVERS 1 FIELD, 2ND INDEX COVERS 1 FIELD, 3RD INDEX COVERS 2 FIELDS)

  Total time: 1.8345210552215576 sec., per sample: 1.8345210552215576e-06 sec., samples per second: 545101.4024361953, total samples: 1000000

  INSERT (HASH, 1ST INDEX COVERS 1 FIELD)

  Total time: 1.781458854675293 sec., per sample: 1.781458854675293e-06 sec., samples per second: 561337.6909467103, total samples: 1000000

  SELECT (HASH, 1ST INDEX COVERS 1 FIELD)

  Total time: 0.8209011554718018 sec., per sample: 8.209011554718018e-07 sec., samples per second: 1218173.458929125, total samples: 1000000

  INSERT (HASH, 1ST INDEX COVERS 1 FIELD, 2ND INDEX COVERS 1 FIELD, 3RD INDEX COVERS 2 FIELDS)

  Total time: 4.138401985168457 sec., per sample: 4.138401985168457e-06 sec., samples per second: 241639.16496847855, total samples: 1000000

  SELECT (HASH, 1ST INDEX COVERS 1 FIELD, 2ND INDEX COVERS 1 FIELD, 3RD INDEX COVERS 2 FIELDS)

  Total time: 1.001291036605835 sec., per sample: 1.001291036605835e-06 sec., samples per second: 998710.628020589, total samples: 1000000

  INSERT (RTREE, 1ST INDEX COVERS 1 FIELD)

  Total time: 9.943094968795776 sec., per sample: 9.943094968795777e-05 sec., samples per second: 10057.230702696503, total samples: 100000

  SELECT (RTREE, 1ST INDEX COVERS 1 FIELD, LIMIT = 5)

  Total time: 11.716284990310669 sec., per sample: 0.00011716284990310669 sec., samples per second: 8535.128676256994, total samples: 100000
