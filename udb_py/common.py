import datetime
import sys
import time
import uuid

from struct import pack


CHAR255 = chr(255)


class ConstraintError(Exception):
    pass


class FieldRequiredError(Exception):
    pass


class InvalidAggregationOperationError(Exception):
    pass


class InvalidScanOperationValueError(Exception):
    pass


class UnknownSeqScanOperationError(Exception):
    pass


class ViewScanFieldOverriddenError(Exception):
    pass


class InfL(object):
    pass


class InfR(object):
    pass


class Empty(object):
    pass


EMPTY = Empty()


def auto_id(generator=lambda: str(uuid.uuid1())):
    return lambda key, record: generator()


def current_timestamp(formatter=int):
    return lambda key, record: formatter(_now().timestamp())


def fn(func):
    return lambda key, record: func(record)


def _now():
    return datetime.datetime.now()


OPTIONAL = None
REQUIRED = EMPTY


class Lst(list):
    def get(self, index, default):
        return self[index] if 0 <= index < len(self) else default

    def update(self, l):
        self.clear()
        self.extend(l)


TYPE_COMPARATORS = {
    InfL: {
        InfL: False,
        bool: False,
        int: False,
        float: False,
        type(None): False,
        str: False,
        InfR: False,
    },
    bool: {
        InfL: False,
        bool: None,
        int: False,
        float: False,
        type(None): True,
        str: False,
        InfR: False,
    },
    int: {
        InfL: False,
        bool: True,
        int: None,
        float: None,
        type(None): True,
        str: False,
    },
    float: {
        InfL: False,
        bool: True,
        int: None,
        float: None,
        type(None): True,
        str: False,
    },
    type(None): {
        InfL: False,
        bool: False,
        int: False,
        float: False,
        type(None): False,
        str: False,
        InfR: False,
    },
    str: {
        InfL: False,
        bool: True,
        int: True,
        float: True,
        type(None): True,
        str: None,
        InfR: False,
    },
}
TYPE_FORMAT_MAPPERS = {
    Empty: lambda x: chr(0),
    InfL: lambda x: chr(0),
    bool: lambda x: '\x02\x01' if x else '\x02\x00',
    int: lambda x: ('\x03\x00' if x < 0 else '\x03\x01') + pack('>q', x).decode('latin'),
    float: None,
    type(None): lambda x: chr(1),
    str: lambda x: chr(4) + x,
    InfR: lambda x: chr(255),
}
TYPE_INFL = chr(0)
TYPE_INFR = chr(255)


def sort_key_iter(key, iterable, reverse=False, type_format_mappers=TYPE_FORMAT_MAPPERS):
    return iter(sorted(
        iterable,
        key=lambda record: type_format_mappers.get(type(record.get(key, None)), type_format_mappers[type(None)])(record.get(key, None)),
        reverse=reverse,
    ))


def sort_iter(iterable, reverse=False, type_format_mappers=TYPE_FORMAT_MAPPERS):
    return iter(sorted(
        iterable,
        key=lambda record: type_format_mappers.get(type(record), type_format_mappers[type(None)])(record),
        reverse=reverse,
    ))


def type_formatter_iter(iterable):
    for val in iterable:
        yield TYPE_FORMAT_MAPPERS[type(val)](val)


def configure_float_precision(precision=18):
    precision_multiplier = 10**precision
    type_format_mapper_int = TYPE_FORMAT_MAPPERS[int]
    type_format_mappers = dict(TYPE_FORMAT_MAPPERS)

    def formatter(x):
        y = int(x)

        return type_format_mapper_int(y) + type_format_mapper_int(int((x - y) * precision_multiplier))

    type_format_mappers[float] = type_format_mappers[int] = formatter

    return type_format_mappers


TYPE_FORMAT_MAPPER_INT_AS_FLOAT = configure_float_precision()


def cpy_dict(dct, update=None):
    dct = dict(dct)

    return upd_dict(dct, update) if update else dct


def upd_dict(dct, update):
    dct.update(update)

    return dct
