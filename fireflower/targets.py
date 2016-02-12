import csv
from contextlib import contextmanager
from io import TextIOWrapper
from gzip import GzipFile

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
    def __init__(self, path, compressed=True, kwargs_in=None, kwargs_out=None,
                 format=None):

        if compressed:
            format = luigi.format.Nop

        self.compressed = compressed
        self.kwargs_in = kwargs_in
        self.kwargs_out = kwargs_out
        super(S3CSVTarget, self).__init__(path, format)

    @staticmethod
    def write_csv_rows(csv_writer, values, header=None):
        if header:
            csv_writer.writerow(header)
        for value in values:
            csv_writer.writerow(value)

    @contextmanager
    def _open_writer(self):
        with self.open('w') as f:
            yield f

    @contextmanager
    def _open_reader(self):
        with self.open('r') as f:
            yield f

    def write_csv_tuples(self, tuples, header_tuple=None):
        """Stream tuples to s3 as a csv
           tuples --  iterable of n-tuples
           header_tuple -- n-tuple that indicates fields for csv
        """
        with self._open_writer() as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='wb')) as g:
                    csv_writer = csv.writer(g)
                    self.write_csv_rows(csv_writer, tuples, header_tuple)
            else:
                csv_writer = csv.writer(f)
                self.write_csv_rows(csv_writer, tuples, header_tuple)

    def write_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self._open_writer() as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='wb')) as g:
                    df.to_csv(g, **kwargs)
            else:
                df.to_csv(f, **kwargs)

    def read_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)

        stream = 'chunksize' in kwargs

        with self._open_reader() as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='rb')) as g:
                    if stream:
                        for chunk in pd.read_csv(g, **kwargs):
                            yield chunk
                    else:
                        return pd.read_csv(g, **kwargs)
            else:
                if stream:
                    for chunk in pd.read_csv(f, **kwargs):
                        yield chunk
                else:
                    return pd.read_csv(f, **kwargs)
