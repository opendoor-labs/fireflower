from datetime import date, datetime, time
import json

import arrow
import requests
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


def get_pending_task_count(scheduler_host, scheduler_port):
    '''
    Hits the Luigi scheduler to retrieve the number of pending tasks.
    This computation is made a little more complex by the fact that Luigi
    treats upstream failed and disabled tasks as still pending. So to get the
    true count of pending tasks, we have to fetch the lists for all three,
    then subtract out the disabled / failed tasks.
    '''

    scheduler_url = f'http://{scheduler_host}:{scheduler_port}/api/task_list'
    pending_filters = ['', 'UPSTREAM_DISABLED', 'UPSTREAM_FAILED']
    responses = [
        requests.get(
            scheduler_url,
            params={'data': json.dumps({'status': 'PENDING', 'upstream_status': p})},
            timeout=10
        )
        for p in pending_filters
    ]
    status_codes = [r.status_code for r in responses]
    if any([code != 200 for code in status_codes]):
        raise RuntimeError(f'Failed to retrive pending task_lists. Response codes: {status_codes}')

    pending_task_id_sets = [set(r.json()['response'].keys()) for r in responses]
    actual_pending_count = len(pending_task_id_sets[0] - pending_task_id_sets[1] - pending_task_id_sets[2])

    return actual_pending_count


def get_running_task_count(scheduler_host, scheduler_port):
    '''
    Gets the running task count from the Luigi scheduler.
    '''

    scheduler_url = f'http://{scheduler_host}:{scheduler_port}/api/task_list'
    response = requests.get(
        scheduler_url,
        params={'data': json.dumps({'status': 'RUNNING'})},
        timeout=10
    )
    if response.status_code != 200:
        raise RuntimeError(f'Failed to retrive running task_lists. Response code: {status_code}')

    return len(response.json()['response'].keys())


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
