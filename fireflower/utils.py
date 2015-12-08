import json
from datetime import date, datetime, time

import arrow
from sqlalchemy import TypeDecorator, TEXT


def is_string(obj):
    return isinstance(obj, str)


def is_boolean(obj):
    return isinstance(obj, bool)


def is_integer(obj):
    return isinstance(obj, int) and not is_boolean(obj)


def is_float(obj):
    return isinstance(obj, float)


def is_numeric(obj):
    return is_float(obj) or is_integer(obj)


def is_datetime(obj):
    # Filter out pandas.NaT
    return isinstance(obj, datetime) and obj == obj


def is_date(obj):
    return isinstance(obj, date) and not isinstance(obj, datetime)


def is_finite(obj):
    return is_numeric(obj) and obj == obj and abs(obj) != float('inf')


def deep_sorted(d):
    if isinstance(d, dict):
        return sorted((k, deep_sorted(v)) for k, v in d.items())
    elif hasattr(d, '__iter__') and not isinstance(d, str):
        return sorted(deep_sorted(v) for v in d)
    else:
        return d


def to_date(obj, default=None, raise_=False):
    if is_datetime(obj) or isinstance(obj, arrow.Arrow):
        return obj.date()
    elif is_date(obj):
        return obj
    elif is_string(obj) or (is_numeric(obj) and is_finite(obj)):
        try:
            return arrow.get(obj).date()
        except (TypeError, RuntimeError):
            pass
    if raise_:
        raise TypeError('could not convert to date: {}'.format(obj))
    return default


def to_datetime(obj, default=None, raise_=False):
    if is_datetime(obj):
        return obj
    elif is_date(obj):
        return datetime.combine(obj, time.min)
    elif is_string(obj) or (is_numeric(obj) and is_finite(obj)):
        try:
            return arrow.get(obj).naive
        except (TypeError, RuntimeError):
            pass
    elif isinstance(obj, arrow.Arrow):
        return obj.naive
    if raise_:
        raise TypeError('could not convert to datetime: {}'.format(obj))
    return default


class JSONEncoded(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::
        JSONEncoded(255)
    """
    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value
