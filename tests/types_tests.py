from io import StringIO
from unittest import TestCase

import numpy as np

from fireflower.targets import read_typed_csv, write_typed_csv
from fireflower.types import FeatureType


class FeatureTypesTests(TestCase):
    def test_string_category_feature(self):
        inp = StringIO("a,b\n1,\n,\n")
        df = read_typed_csv(inp, {'a': FeatureType.str_category})

        self.assertEqual(df.a.iloc[0], '1')
        self.assertTrue(np.isnan(df.a.iloc[1]))
        self.assertEqual(df.dtypes['a'], np.dtype(object))

        out = StringIO()
        write_typed_csv(out, df, {'a': FeatureType.str_category}, index=False)
        self.assertEqual(out.getvalue(), 'a,b\n1,\n,\n')

    def test_int_category_feature(self):
        inp = StringIO("a,b\n1,\n,\n")
        df = read_typed_csv(inp, {'a': FeatureType.int_category})

        self.assertEqual(df.a.iloc[0], 1.0)
        self.assertTrue(np.isnan(df.a.iloc[1]))
        self.assertEqual(df.dtypes['a'], np.dtype(float))

        out = StringIO()
        write_typed_csv(out, df, {'a': FeatureType.int_category}, index=False)
        self.assertEqual(out.getvalue(), 'a,b\n1,\n,\n')

    def test_bool_category_feature(self):
        inp = StringIO("a,b,c\ntrue,,true\n,,false\nfalse,,true\n")
        df = read_typed_csv(inp, {'a': FeatureType.bool,
                                  'c': FeatureType.bool})

        self.assertEqual(df.a.iloc[0], True)
        self.assertTrue(np.isnan(df.a.iloc[1]))
        self.assertEqual(df.a.iloc[2], False)
        self.assertEqual(df.dtypes['a'], np.dtype(object))

        df['a'] = df.a.astype(float)  # check bool type with null
        df['c'] = df.c.astype(bool)   # check bool col w/o null

        out = StringIO()
        write_typed_csv(out, df, {'a': FeatureType.bool,
                                  'c': FeatureType.bool}, index=False)
        self.assertEqual(out.getvalue(), "a,b,c\nTrue,,True\n,,False\nFalse,,True\n")
