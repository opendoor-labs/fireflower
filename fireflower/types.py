import numpy as np
import pandas as pd


class FeatureType(object):
    """
    FeatureType determines how datatypes are stored internally in Series
    objects, and how they're serialized to .csv.

    invariants:
    - serialized representation is unique (no '1' vs '1.0')
    - no type conversion occurs upon round-tripping a feature to .csv

    in-memory storage format should generally accomodate NaNs; this rules out
    bool and int dtypes, so many things are stored as floats internally and then
    converted on serialization/deserialization.
    """

    @property
    def serialization_dtype(self):
        """ Returns the dtype which should be used for pd.read_csv().
            This may not match the eventual dtype of the feature, it's
            an implementation detail of read/write_typed_csv.
        """
        raise NotImplementedError

    def input(self, series):
        """ Performs any necessary post-processing after
            deserializating a column from .csv.
        :type series: pd.Series with dtype == serialization_dtype.
        :rtype pd.Series with the desired dtype of the feature.
        """
        return series

    def output(self, series):
        """ Pre-process a column before calling .to_csv()
        :rtype pd.Series
        """
        return series

    def scalar_to_str(self, value):
        """ Converts a single value to the serialized representation.
        :type value: ideally, the type used for internal in-memory
        """
        return (self.output(
            pd.Series([value],
                      dtype=self.serialization_dtype))
            .astype(str)
            .iloc[0])

    def empty_series(self, index):
        """ returns an empty Series of the appropriate type """
        return self.input(
            pd.Series(index=index, dtype=self.serialization_dtype))


class FloatFeature(FeatureType):
    serialization_dtype = np.float64

    def output(self, series):
        return series.astype(float)

FeatureType.float = FloatFeature()


class StringFeature(FeatureType):
    serialization_dtype = np.str_
FeatureType.str = StringFeature()


class IntegerFeature(FeatureType):
    """ Integer feature. Stored in-memory as float so NAs are representable. """
    serialization_dtype = np.float64

    def output(self, series):
        # convert the non-NA values to integer, with dtype object.
        # We jump through this hoop since dtype=int arrays can't have
        # NAs, but we want to record the int values in the output .csv.
        return pd.Series(index=series.index,
                         data=[int(x) if pd.notnull(x) else x for x in series],
                         dtype=object)
FeatureType.int = IntegerFeature()


class IntNonNullFeature(FeatureType):
    """ Integer feature in memory int, when NULLs are not possible. """
    serialization_dtype = np.int
FeatureType.int_non_null = IntNonNullFeature()


class BooleanFeature(FeatureType):
    """ boolean features are stored in pandas with dtype 'object',
        if they contain NAs, and 'bool' dtype if it doesn't.
        the scalar object types ends up mixed (bool or np.NaN::float).

        if you ask for bool, read_csv may return 'object' or 'bool',
        depending, or those if you ask for bool dtype. so we always
        set dtype to object to avoid data-dependent types. """
    @property
    def serialization_dtype(self):
        return np.bool

    def output(self, series):
        '''Handles output of series with null value properly, e.g.
        instead of outputing [True,False, np.nan] as "1.0,0.0,"
        it should output "True,False,"'''

        if series.dtype == np.dtype(float):
            # convert Series of (1.0, 0.0, and nan) to (True, False, nan)
            series = series.copy().astype(object)
            series[series == 1.0] = True
            series[series == 0.0] = False
            return series
        else:
            return series

    def input(self, series):
        return series.astype(object)
FeatureType.bool = BooleanFeature()


class StringCategoryFeature(StringFeature):
    """ This type should be used for enum-like strings (i.e. a fixed, small
        number of possibilities, as opposed to free-form text. Plan is to use
        pandas Categories for these.
    """
    pass
FeatureType.str_category = StringCategoryFeature()


class IntegerCategoryFeature(IntegerFeature):
    """ Integer feature drawn from a small number of possibilities """
    pass
FeatureType.int_category = IntegerCategoryFeature()


class DateFeature(FeatureType):
    serialization_dtype = np.str_

    def input(self, series):
        return pd.to_datetime(series).dt.date
FeatureType.date = DateFeature()


class DatetimeFeature(FeatureType):
    serialization_dtype = np.str_

    def input(self, series):
        return pd.to_datetime(series)
FeatureType.datetime = DatetimeFeature()
