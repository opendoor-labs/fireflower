import base64
import io
import tempfile
from contextlib import contextmanager

import luigi
from luigi.s3 import S3Target
import pandas as pd
import toolz

from fireflower.core import FireflowerStateManager
from fireflower.models import TaskOutput

__all__ = [
    'DBTaskOutputTarget',
    'S3Target',
    'S3CSVTarget'
]


class DBTaskOutputTarget(luigi.Target):
    """ Target class which writes a row to signals.task_outputs """
    @classmethod
    def create(cls, task):
        return cls(task_id=task.task_id,
                   task_family=task.task_family,
                   params=task.to_str_params())

    def __init__(self, task_id, task_family, params):
        self._db_session = FireflowerStateManager.session
        self._task_id = task_id
        self._task_family = task_family
        self._params = params

    @contextmanager
    def _session(self):
        try:
            yield self._db_session
        except Exception:
            self._db_session.rollback()
            raise
        else:
            self._db_session.commit()

    def touch(self):
        self.write(None)

    def _get_query(self, task_session):
        return (task_session.query(TaskOutput)
                .filter(TaskOutput.task_id == self._task_id))

    def write(self, value):
        with self._session() as task_session:
            task_output = TaskOutput(task_id=self._task_id,
                                     value=value,
                                     task_family=self._task_family,
                                     params=self._params)
            task_session.add(task_output)

    def read(self):
        with self._session() as task_session:
            return self._get_query(task_session).one().value

    def exists(self):
        with self._session() as task_session:
            return (task_session.query(self._get_query(task_session).exists())
                    .scalar())

    def remove(self):
        with self._session() as task_session:
            self._get_query(task_session).delete()

    def upsert(self, value):
        with self._session() as task_session:
            task_output = self._get_query(task_session).one_or_none()
            if task_output is None:
                task_output = TaskOutput(task_id=self._task_id,
                                         value=value,
                                         task_family=self._task_family,
                                         params=self._params)
                task_session.add(task_output)
            else:
                task_output.value = value


class S3CSVTarget(S3Target):
    def __init__(self, path, compressed=True, kwargs_in=None, kwargs_out=None):
        self.compressed = compressed
        self.kwargs_in = kwargs_in
        self.kwargs_out = kwargs_out
        super(S3CSVTarget, self).__init__(path)

    @contextmanager
    def _open_writer(self):
        with self.open('w') as f:
            yield f

    @contextmanager
    def _open_reader(self):
        with self.open('r') as f:
            yield f

    def write_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self._open_writer() as f:
            if self.compressed:
                with tempfile.NamedTemporaryFile(mode='w+b') as tmp_w:
                    df.to_csv(tmp_w.name, compression='gzip', **kwargs)
                    with open(tmp_w.name, 'rb') as tmp_r:
                        bytes_ = tmp_r.read()
                        encoded_bytes = base64.b64encode(bytes_)
                        f.write(str(encoded_bytes, 'utf-8'))
            else:
                df.to_csv(f, **kwargs)

    def read_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)
        with self._open_reader() as f:
            if self.compressed:
                encoded_bytes = bytes(f.read(), 'utf-8')
                bytes_ = base64.b64decode(encoded_bytes)
                byte_stream = io.BytesIO(bytes_)
                return pd.read_csv(byte_stream, compression='gzip', **kwargs)
            else:
                return pd.read_csv(f, **kwargs)
