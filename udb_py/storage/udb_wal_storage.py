import json
import logging
import os
import struct

from ..common import PYTHON2
from ..udb_storage import UdbStorage


_DELETE_OP = 0
_INSERT_OP = 1
_UPDATE_OP = 2


class CorruptedWalError(Exception):
    pass


class FSWalError(Exception):
    pass


class UdbWalStorage(UdbStorage):
    _allow_corrupted_wal = False
    _file_wal = None
    _name = None
    _revision = -1

    def __init__(self, name, allow_corrupted_wal=False):
        self._allow_corrupted_wal = allow_corrupted_wal
        self._name = name

    def is_available(self):
        return os.path.isfile(self._name + '.wal.data') or os.path.isfile(self._name + '.wal.data.bak')

    def is_capture_events(self):
        return True

    def drop(self):
        self._wal_close()

        if os.path.isfile(self._name + '.wal.data'):
            os.remove(self._name + '.wal.data')

        if os.path.isfile(self._name + '.wal.data.bak'):
            os.remove(self._name + '.wal.data.bak')

        if os.path.isfile(self._name + '.wal.meta'):
            os.remove(self._name + '.wal.meta')

        return self

    def load(self):
        self._wal_close()

        self._revision = -1

        indexes = {}

        if self.is_available():
            if os.path.isfile(self._name + '.wal.meta'):
                with open(self._name + '.wal.meta', 'r') as file_r_desc:
                    meta = json.load(file_r_desc)

                indexes = meta['indexes']

            if not os.path.isfile(self._name + '.wal.data.bak'):
                with open(self._name + '.wal.data', 'rb') as file_r_desc:
                    with open(self._name + '.wal.data.bak', 'wb') as file_w_desc:
                        chunk = file_r_desc.read(1024 * 1024 * 16)

                        while chunk:
                            if file_w_desc.write(chunk) != len(chunk):
                                if not PYTHON2:
                                    os.remove(self._name + '.wal.data.bak')

                                    raise FSWalError(self._name + '.wal.data')

                            chunk = file_r_desc.read(1024 * 1024 * 16)

            logging.debug('%s.wal.data replaying', self._name)

            collection, del_upd_count = self._wal_read(open(self._name + '.wal.data.bak', 'rb'))

            logging.debug('%s.wal.data replayed, %i total records', self._name, len(collection))

            if del_upd_count or not os.path.isfile(self._name + '.wal.data'):
                logging.debug('%s.wal.data repacking', self._name)

                self._file_wal = open(self._name + '.wal.data', 'wb+')

                for rid, record in collection.items():
                    self.on_insert(int(rid), record)

                self._wal_close()

                logging.debug('%s.wal.data repacked', self._name)

            os.remove(self._name + '.wal.data.bak')
        else:
            collection = {}

        self._wal_open()

        return {'indexes': indexes, 'revision': self._revision + 1, 'data': collection}

    def save(self, indexes, revision, data):
        return self

    def save_meta(self, indexes, revision):
        with open(self._name + '.wal.meta', 'w+') as file_w_desc:
            json.dump({
                'indexes': {k: [v.schema_keys, v.type] for k, v in indexes.items()},
                'revision': revision,
                'data': [],
            }, file_w_desc, indent=2)

        return True

    def on_delete(self, rid):
        if self._file_wal:
            self._wal_write(_DELETE_OP, rid)

        return self

    def on_insert(self, rid, record):
        if self._file_wal:
            self._wal_write(_INSERT_OP, rid, record)

        return self

    def on_update(self, rid, record, values):
        if self._file_wal:
            self._wal_write(_UPDATE_OP, rid, record, values)

        return self

    def _wal_close(self):
        if self._file_wal:
            self._file_wal.close()

            self._file_wal = None

    def _wal_open(self):
        if not self._file_wal:
            self._file_wal = open(self._name + '.wal.data', 'ab+')

        return self._file_wal

    def _wal_read(self, file_descriptor):
        collection = {}
        del_upd_count = 0

        while True:
            chunk = file_descriptor.read(9)

            if chunk and len(chunk) < 9:
                self._wal_corrupted_warn()

                return collection, del_upd_count

            if not chunk:
                file_descriptor.close()

                return collection, del_upd_count

            operation, rid, _ = struct.unpack('BIB', chunk)

            if operation == _DELETE_OP:
                collection.pop(rid, None)

                del_upd_count += 1
            else:
                if operation in (_INSERT_OP, _UPDATE_OP):
                    chunk = file_descriptor.read(4)

                    if len(chunk) < 4:
                        self._wal_corrupted_warn()

                        return collection, del_upd_count

                    record_length = struct.unpack('I', chunk)[0]

                    chunk = file_descriptor.read(record_length)

                    if len(chunk) < record_length:
                        self._wal_corrupted_warn()

                        return collection, del_upd_count

                    record = struct.unpack(str(record_length) + 's', chunk)[0]

                    collection[rid] = json.loads(record.decode('utf-8'))
                    collection[rid]['__rev__'] = rid

                    self._revision = rid

                if operation == _UPDATE_OP:
                    chunk = file_descriptor.read(4)

                    if len(chunk) < 4:
                        self._wal_corrupted_warn()

                        return collection, del_upd_count

                    record_length = struct.unpack('I', chunk)[0]

                    chunk = file_descriptor.read(record_length)

                    if len(chunk) < record_length:
                        self._wal_corrupted_warn()

                        return collection, del_upd_count

                    record = struct.unpack(str(record_length) + 's', chunk)[0]

                    collection[rid].update(json.loads(record.decode('utf-8')))
                    collection[rid]['__rev__'] = rid

                    del_upd_count += 1

    def _wal_write(self, operation, rid, *values):
        packed = struct.pack('BIB', operation, rid, len(values))

        if self._file_wal.write(packed) != len(packed):
            if not PYTHON2:
                raise FSWalError()

        for val in values:
            val = json.dumps(val).encode('utf-8')

            packed = struct.pack('I' + str(len(val)) + 's', len(val), val)

            if self._file_wal.write(packed) != len(packed):
                if not PYTHON2:
                    raise FSWalError()

    def _wal_corrupted_warn(self):
        if self._allow_corrupted_wal:
            logging.warning('%s.wal.data is corrupted', self._name)
        else:
            raise CorruptedWalError(self._name)
