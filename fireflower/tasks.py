import hashlib
from datetime import datetime, timedelta

import luigi
from luigi.task_register import Register

from fireflower.core import FireflowerStateManager, \
    luigi_run_wrapper
from fireflower.parameters import SignatureParameter
from fireflower.targets import DBTaskOutputTarget
from fireflower.utils import to_date, to_datetime, deep_sorted

__all__ = [
    'FireflowerTask',
    'FireflowerOutputTask',
    'DateParameterTask',
    'DateHourParameterTask',
    'SignatureTask',
]


class FireflowerLuigiMeta(Register):
    """
    Metaclass to wrap the LuigiTask.run method, for sentry & logging.
    """
    def __call__(cls, *args, **kwargs):
        if not cls._is_run_method_wrapped_by_fireflower:
            cls.run = luigi_run_wrapper(cls.run)
            cls._is_run_method_wrapped_by_fireflower = True
        return super(FireflowerLuigiMeta, cls).__call__(*args, **kwargs)


class FireflowerTask(luigi.Task, metaclass=FireflowerLuigiMeta):
    _is_run_method_wrapped_by_fireflower = False


class FireflowerOutputTask(FireflowerTask):
    """
    Base luigi task class for using the signals task_outputs table to mark
    task completion and to also communicate return values to downstream tasks.
    """
    def output(self):
        return DBTaskOutputTarget.create(self)


class DateParameterTask(FireflowerTask):
    """
    Convenience mixin to provide start_date and end_date luigi parameters.
    This mixin also provides a handy conversion to string types (e.g. for tasks that accept date strings only).
    """
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        self.start = to_date(kwargs.get('start_date'), raise_=True)
        self.end = to_date(kwargs.get('end_date'), raise_=True)
        super(DateParameterTask, self).__init__(*args, **kwargs)

    @property
    def start_date_str(self):
        return self.start.strftime('%Y-%m-%d')

    @property
    def end_date_str(self):
        return self.end.strftime('%Y-%m-%d')


class DateHourParameterTask(FireflowerTask):
    start_datetime = luigi.DateHourParameter()
    end_datetime = luigi.DateHourParameter()

    def __init__(self, *args, **kwargs):
        self.start = to_date(kwargs.get('start_datetime'), raise_=True)
        self.end = to_date(kwargs.get('end_datetime'), raise_=True)
        super(DateHourParameterTask, self).__init__(*args, **kwargs)

    @property
    def start_date_str(self):
        return self.start.strftime('%Y-%m-%d')

    @property
    def end_date_str(self):
        return self.end.strftime('%Y-%m-%d')

    @property
    def start_datetime_str(self):
        return self.start.strftime('%Y-%m-%d %H:00')

    @property
    def end_datetime_str(self):
        return self.end.strftime('%Y-%m-%d %H:00')


class SignatureTask(FireflowerTask):
    def __init__(self, *args, **kwargs):
        super(SignatureTask, self).__init__(*args, **kwargs)

        param_objs = self.get_params()    # [(param_name, param_obj)]
        param_values = self.param_kwargs  # {param_name: param_value}

        task_id_parts = []
        for name, obj in param_objs:
            if obj.significant:
                if isinstance(obj, SignatureParameter):
                    value = hashlib.md5(str(
                        deep_sorted(param_values[name])).encode()).hexdigest()
                else:
                    value = obj.serialize(param_values[name])
                task_id_parts.append('%s=%s' % (name, value))

        self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))
