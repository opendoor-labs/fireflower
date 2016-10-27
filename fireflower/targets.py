import os
import csv
from contextlib import contextmanager
from io import TextIOWrapper
from gzip import GzipFile

import luigi
import structlog
from luigi.file import LocalTarget
from luigi.s3 import S3Target
from luigi.target import FileSystemTarget
import pandas as pd
import toolz

from fireflower.core import FireflowerStateManager
from fireflower.models import TaskOutput
from fireflower.profiler import profile_method

__all__ = [
    'DBTaskOutputTarget',
    'S3Target',
    'S3CSVTarget'
]

logger = structlog.get_logger(__name__)


class FireflowerS3Target(FileSystemTarget):
    """ Operates the same way as S3Target, except it looks for an environment variable LOCAL_S3_PATH
    and kwarg named local_s3_path, which is a path on your local machine to store s3 files. If this
    is set, the target will read / write to this path by stripping off s3:// and following the rest
    of the path. Supports any format supported by FileSystemTarget.
    """

    def __init__(self, path, *args, **kwargs):
        self.local_s3_path = kwargs.pop('local_s3_path', os.getenv('LOCAL_S3_PATH', None))
        if not self.local_s3_path:
            self._proxy = S3Target(path, *args, **kwargs)
        else:
            path = os.path.join(self.local_s3_path, path.replace('s3://', ''))
            self._proxy = LocalTarget(path, *args, **kwargs)

    @property
    def path(self):
        return self._proxy.path

    @property
    def fs(self):
        return self._proxy.fs

    def open(self, mode='r'):
        return self._proxy.open(mode)


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
                                     params=self._params,
                                     param_dict=self._params)
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


class CSVStream:
    def __init__(self, csv_writer, *closeables):
        self.closeables = closeables
        self.csv_writer = csv_writer

    def write_tuple(self, tpl):
        self.csv_writer.writerow(tpl)

    def write_tuples(self, tpls):
        for v in tpls:
            self.csv_writer.writerow(v)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for file in self.closeables:
            file.close()


class CSVInStream:
    def __init__(self, csv_reader, *closeables):
        self.closeables = closeables
        self.csv_reader = csv_reader

    def __iter__(self):
        return self.csv_reader

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for file in self.closeables:
            file.close()


class S3CSVTarget(FireflowerS3Target):
    def __init__(self, path, compressed=True, kwargs_in=None, kwargs_out=None,
                 format=None, client=None):

        if compressed:
            format = luigi.format.Nop

        self.compressed = compressed
        self.kwargs_in = kwargs_in
        self.kwargs_out = kwargs_out
        super(S3CSVTarget, self).__init__(path, format, client)

    @staticmethod
    def write_values(csv_writer, values, header=None):
        if header:
            csv_writer.writerow(header)
        for v in values:
            csv_writer.writerow(v)

    def open_csv_stream(self):
        """ Returns a CSVStream object (a context manager) for streaming tuples
            to the CSV.
        """
        f = self.open('w')
        if self.compressed:
            iostream = TextIOWrapper(GzipFile(fileobj=f, mode='wb'))
            csv_writer = csv.writer(iostream)
            return CSVStream(csv_writer, iostream, f)
        else:
            csv_writer = csv.writer(f)
            return CSVStream(csv_writer, f)

    def write_csv_tuples(self, tuples, header_tuple=None):
        """Stream tuples to s3 as a csv
           tuples --  iterable of n-tuples
           header_tuple -- n-tuple that indicates fields for csv
        """
        with self.open('w') as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='wb')) as g:
                    csv_writer = csv.writer(g)
                    self.write_values(csv_writer, tuples, header_tuple)
            else:
                csv_writer = csv.writer(f)
                self.write_values(csv_writer, tuples, header_tuple)

    @profile_method(logger)
    def write_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self.open('w') as f:
            if self.compressed:
                encoding = kwargs.get('encoding', 'utf-8')
                with TextIOWrapper(GzipFile(fileobj=f, mode='wb'), encoding=encoding) as g:
                    df.to_csv(g, **kwargs)
            else:
                df.to_csv(f, **kwargs)

    def read_csv_stream(self, **kwargs):
        """
            uses panda dataframe chunksize to stream a pandas df in chunks
            chunksize should be greater than 1 to avoid header issues.
            separate function from read_csv to avoid conflicting return types
        """
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)

        # default to 2 in case for headers
        kwargs.setdefault('chunksize', 2)

        with self.open('r') as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='rb')) as g:
                    for chunk in pd.read_csv(g, **kwargs):
                        yield chunk
            else:
                for chunk in pd.read_csv(f, **kwargs):
                    yield chunk

    def open_csv_dict_stream(self, **kwargs):
        f = self.open('r')
        if self.compressed:
            g = TextIOWrapper(GzipFile(fileobj=f, mode='rb'))
            csv_reader = csv.DictReader(g)
            return CSVInStream(csv_reader, f, g)
        else:
            csv_reader = csv.DictReader(f)
            return CSVInStream(csv_reader, f)

    def read_csv_dict_stream(self, **kwargs):
        with self.open_csv_dict_stream(**kwargs) as stream:
            yield from stream

    @profile_method(logger)
    def read_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)

        with self.open('r') as f:
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='rb')) as g:
                    return pd.read_csv(g, **kwargs)
            else:
                return pd.read_csv(f, **kwargs)


# TODO(nelson): refactor to reduce code duplication with S3CSVTarget
class S3TypedCSVTarget(S3CSVTarget):
    def __init__(self, path, types, compressed=True,
                 kwargs_in=None, kwargs_out=None, format=None, client=None):
        self.types = types
        if compressed:
            format = luigi.format.Nop

        super(S3TypedCSVTarget, self).__init__(path, compressed,
                                               kwargs_in, kwargs_out, format, client)

    def write_typed_csv(self, df, **kwargs):
        if self.kwargs_out:
            kwargs = toolz.merge(self.kwargs_out, kwargs)
        with self.open('w') as f:
            transformed = pd.DataFrame.from_items(
               (colname,
                   self.types[colname].output(col)
                   if colname in self.types else col)
               for colname, col in df.items())

            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='wb')) as g:
                    transformed.to_csv(g, compression='gzip', **kwargs)
            else:
                transformed.to_csv(f, **kwargs)

    def read_typed_csv(self, **kwargs):
        if self.kwargs_in:
            kwargs = toolz.merge(self.kwargs_in, kwargs)
        with self.open('r') as f:
            dtype = {colname: coltype.serialization_dtype
                     for colname, coltype in self.types.items()}
            if self.compressed:
                with TextIOWrapper(GzipFile(fileobj=f, mode='rb')) as g:
                    df = pd.read_csv(filepath_or_buffer=g,
                                     dtype=dtype,
                                     **kwargs)
            else:
                df = pd.read_csv(f, dtype=dtype, **kwargs)

            return pd.DataFrame.from_items(
                    (colname,
                    self.types[colname].input(col) if colname in
                                                      self.types else col)
                    for colname, col in df.items())


def read_typed_csv(input, types, *args, **kwargs):
    inp = pd.read_csv(input,
                      dtype={colname: coltype.serialization_dtype
                             for colname, coltype in types.items()},
                      *args,
                      **kwargs)

    return pd.DataFrame.from_items(
        (colname,
         types[colname].input(col) if colname in types else col)
        for colname, col in inp.items())


def write_typed_csv(output, df, types, *args, **kwargs):
    transformed = pd.DataFrame.from_items(
        (colname,
         types[colname].output(col) if colname in types else col)
        for colname, col in df.items())
    transformed.to_csv(output, *args, **kwargs)
