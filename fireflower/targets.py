import gzip
import os.path
import shutil
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
    'S3CSVTarget',
    'S3TypedCSVTarget',
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
            if self.compressed:
                with gzip.GzipFile(filename=os.path.basename(self.path),
                                   fileobj=f) as gzfile:
                    yield gzfile
            else:
                yield f

    @contextmanager
    def _open_reader(self):
        with self.open('r') as rawf:
            if self.compressed:
                with tempfile.TemporaryFile(mode='rw+b') as tmpfile:
                    # gzip doesn't cooperate with luigi's S3 fake file
                    # object so we need to copy to tmp
                    shutil.copyfileobj(rawf, tmpfile)
                    tmpfile.seek(0)
                    yield tmpfile
            else:
                yield rawf

    def write_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self._open_writer() as f:
            df.to_csv(f, **kwargs)

    def read_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)
        with self._open_reader() as f:
            return pd.read_csv(f,
                               compression='gzip' if self.compressed else None,
                               **kwargs)


class S3TypedCSVTarget(S3CSVTarget):
    def __init__(self, path, types, compressed=True, kwargs_in=None,
                 kwargs_out=None):
        self.types = types
        super(S3TypedCSVTarget, self).__init__(path, compressed,
                                               kwargs_in, kwargs_out)

    def write_typed_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self._open_writer() as f:
            write_typed_csv(f, df, self.types, **kwargs)

    def read_typed_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)
        with self._open_reader() as f:
            return read_typed_csv(f,
                                  self.types,
                                  compression='gzip' if self.compressed else None,
                                  **kwargs)


def read_typed_csv(input, types, *args, **kwargs):
    inp = pd.read_csv(input,
                      dtype={colname: coltype.serialization_dtype
                             for colname, coltype in types.iteritems()},
                      *args,
                      **kwargs)

    return pd.DataFrame.from_items(
        (colname,
         types[colname].input(col) if colname in types else col)
        for colname, col in inp.iteritems())


def write_typed_csv(output, df, types, *args, **kwargs):
    transformed = pd.DataFrame.from_items(
        (colname,
         types[colname].output(col) if colname in types else col)
        for colname, col in df.iteritems())
    transformed.to_csv(output, *args, **kwargs)
