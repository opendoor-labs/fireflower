import boto
import os
import pandas as pd

from unittest import TestCase, mock

from moto import mock_s3

from fireflower.targets import S3CSVTarget, FireflowerS3Target, S3TypedCSVTarget
from nose_parameterized import parameterized
from testfixtures import TempDirectory

from fireflower.types import FeatureType


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

    def test_local_file(self):
        with TempDirectory() as d:

            def getenv(v, _):
                # Is there a better way to assert arguments?
                self.assertEqual(v, 'LOCAL_S3_PATH')
                return d.path

            with mock.patch('fireflower.targets.os.getenv',
                            side_effect=getenv):

                s = FireflowerS3Target('some_file.txt')
                with s.open('w') as fout:
                    fout.write('asdf')
                    fout.write('fdsa')
                with s.open('r') as fin:
                    expected_text = fin.read()
                    self.assertEqual("asdffdsa", expected_text)

    @mock_s3
    def test_s3_typed_compressed_csv_target(self):
        conn = boto.connect_s3()
        bucket_name = 'some_bucket'
        file_name = 'some_file.csv.gz'
        dest_path = 's3://%s/%s' % (bucket_name, file_name)
        conn.create_bucket(bucket_name)
        df = pd.DataFrame(index=range(1), data={'a': [1]})
        types = {'a': FeatureType.int}
        s = S3TypedCSVTarget(dest_path, types, compressed=True)
        s.write_csv(df, index=False)
        read_result = s.read_csv()
        self.assertDictEqual(df.to_dict(), read_result.to_dict())

    @mock_s3
    def test_s3_typed_uncompressed_csv_target(self):
        conn = boto.connect_s3()
        bucket_name = 'some_bucket'
        file_name = 'some_file.csv'
        dest_path = 's3://%s/%s' % (bucket_name, file_name)
        conn.create_bucket(bucket_name)
        df = pd.DataFrame(index=range(1), data={'a': [1]})
        types = {'a': FeatureType.int}
        s = S3TypedCSVTarget(dest_path, types, compressed=False)
        s.write_csv(df, index=False)
        read_result = s.read_csv()
        self.assertDictEqual(df.to_dict(), read_result.to_dict())

    @mock_s3
    def test_local_exists(self):
        """
            ensures that the existence of targets is calculated correctly.
            s3 and local existence should not impact one another.
        """
        with TempDirectory() as d:
            # create a file on s3
            conn = boto.connect_s3()
            bucket_name = 'some_bucket'
            file_name = 'some_file.csv.gz'
            dest_path = 's3://%s/%s' % (bucket_name, file_name)
            conn.create_bucket(bucket_name)
            s = S3CSVTarget(dest_path, compressed=False)

            # assert file does not exist on s3
            self.assertFalse(s.exists())

            df = pd.DataFrame(index=range(1), data={'a': [1]})
            s.write_csv(df, index=False)

            # assert that the file exists on s3
            self.assertTrue(s.exists())

            def getenv(v, _):
                # Is there a better way to assert arguments?
                self.assertEqual(v, 'LOCAL_S3_PATH')
                return d.path

            with mock.patch('fireflower.targets.os.getenv',
                            side_effect=getenv):
                # even though it exists on s3, check that it doesnt exist locally
                t = S3CSVTarget(dest_path, compressed=False, local_s3_path=d.path)
                self.assertFalse(t.exists())

                # create the file locally
                os.mkdir(os.path.join(d.path, 'some_bucket'))
                t.write_csv(df, index=False)

                # assert that file exists locally
                self.assertTrue(t.exists())

