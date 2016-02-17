import boto
import pandas as pd

from unittest import TestCase, mock

from moto import mock_s3

from fireflower.targets import S3CSVTarget
from nose_parameterized import parameterized
from testfixtures import TempDirectory


class TargetsTests(TestCase):

    @parameterized.expand([
        (True, True),
        (False, True),
        (True, False),
        (False, False)
    ])
    @mock_s3
    def test_s3_tuple(self, compressed, stream):
        conn = boto.connect_s3()
        bucket_name = 'some_bucket'
        file_name = 'some_file.csv.gz'
        dest_path = 's3://%s/%s' % (bucket_name, file_name)
        conn.create_bucket(bucket_name)

        s = S3CSVTarget(dest_path, compressed=compressed)
        tuples = [(1, 2), (3, 4)]
        s.write_csv_tuples(tuples, ('x', 'y'))
        if stream:
            read_result_stream = s.read_csv_stream()
            read_result = pd.concat(read_result_stream)
        else:
            read_result = s.read_csv()

        exp_dict = {'x': {0: 1, 1: 3}, 'y': {0: 2, 1: 4}}
        self.assertDictEqual(read_result.to_dict(), exp_dict)

    @parameterized.expand([
        (True, True),
        (True, False),
        (False, True),
        (False, False)
    ])
    @mock_s3
    def test_s3_csv(self, compressed, stream):
        conn = boto.connect_s3()
        bucket_name = 'some_bucket'
        file_name = 'some_file.csv.gz'
        dest_path = 's3://%s/%s' % (bucket_name, file_name)
        conn.create_bucket(bucket_name)
        s = S3CSVTarget(dest_path, compressed=compressed)
        df = pd.DataFrame(index=range(1), data={'a': [1]})
        s.write_csv(df, index=False)
        if stream:
            combined_result = s.read_csv_stream(chunksize=5)
            read_result = pd.concat(combined_result)
        else:
            read_result = s.read_csv()
        self.assertDictEqual(df.to_dict(), read_result.to_dict())

    @parameterized.expand([
        (True, True),
        (False, True),
        (True, False),
        (False, False)
    ])
    def test_local_csv(self, compressed, stream):
        with TempDirectory() as d:

            def getenv(v, _):
                # Is there a better way to assert arguments?
                self.assertEqual(v, 'LOCAL_S3_PATH')
                return d.path

            with mock.patch('fireflower.targets.os.getenv',
                            side_effect=getenv):
                s = S3CSVTarget('s3://test.csv.gz', compressed=compressed)
                df = pd.DataFrame(index=range(1), data={'a': [1]})
                s.write_csv(df, index=False)
                if stream:
                    combined_result = s.read_csv_stream(chunksize=5)
                    read_result = pd.concat(combined_result)
                else:
                    read_result = s.read_csv()
                self.assertDictEqual(df.to_dict(), read_result.to_dict())

    @parameterized.expand([
        (True, True),
        (False, True),
        (True, False),
        (False, False)
    ])
    def test_local_tuple(self, compressed, stream):
        with TempDirectory() as d:

            def getenv(v, _):
                # Is there a better way to assert arguments?
                self.assertEqual(v, 'LOCAL_S3_PATH')
                return d.path

            with mock.patch('fireflower.targets.os.getenv',
                            side_effect=getenv):

                tuples = [(1, 2), (3, 4)]
                s = S3CSVTarget('some_file.csv', compressed=compressed)
                s.write_csv_tuples(tuples, ('x', 'y'))
                if stream:
                    combined_result = s.read_csv_stream(chunksize=5)
                    read_result = pd.concat(combined_result)
                else:
                    read_result = s.read_csv()

                exp_dict = {'x': {0: 1, 1: 3}, 'y': {0: 2, 1: 4}}
                self.assertDictEqual(read_result.to_dict(), exp_dict)

