import warnings
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from numpy import empty as np_empty
from pandas.testing import assert_frame_equal

from fastparquet.dataframe import empty

DatetimeTZDtype = pd.DatetimeTZDtype


def test_empty():
    n = 100
    df, views = empty('category', size=n, cols=['c'])
    assert df.shape == (n, 1)
    assert df.dtypes.tolist() == ['category']
    assert views['c'].dtype == 'int16'

    df, views = empty('category', size=n, cols=['c'], cats={'c': 2**20})
    assert df.shape == (n, 1)
    assert df.dtypes.tolist() == ['category']
    assert views['c'].dtype == 'int32'

    df, views = empty('category', size=n, cols=['c'],
                      cats={'c': ['one', 'two']})
    views['c'][0] = 1
    assert df.c[:2].tolist() == ['two', np.nan]

    df, views = empty('i4,i8,f8,f8,O', size=n,
                      cols=['i4', 'i8', 'f8_1', 'f8_2', 'O'])
    assert df.shape == (n, 5)
    assert len(views) == 5


def test_no_cats():
    df, views = empty('category', size=10, cols=['c'],
                      cats={'c': []})
    assert (views["c"] == -1).all()


def test_empty_tz_utc():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        empty([DatetimeTZDtype(unit="ns", tz="UTC")], 10, cols=['a'],
              timezones={'a': 'UTC'})


# non regression test for https://github.com/dask/fastparquet/issues/532
def np_empty_mock(shape, dtype):
    """mock numpy empty to return an initialised array with all hours in 2020 if shape is 365 and dtype.kind is M.
    The objective is to simulate a numpy.empty that returns an uninitialized array with random content that
    can cause issues when tz_localize is applied with a timezone with DST"""
    import numpy
    dtype = numpy.dtype(dtype)
    if shape == 8784 and dtype.kind == "M":
        a = numpy.arange(start="2020-01-01", stop="2021-01-01", dtype="M8[h]").astype(dtype)
    else:
        a = np_empty(shape, dtype)
    return a


@mock.patch("numpy.empty", np_empty_mock)
def test_empty_tz_nonutc():
    df, views = empty(types=[DatetimeTZDtype(unit="ns", tz="CET")], size=8784, cols=['a'],
                      timezones={'a': 'CET', 'index': 'CET'}, index_types=["datetime64[ns]"], index_names=["index"])
    assert str(df.index.tz) == "CET"
    assert str(df.a.dtype.tz) == "CET"


# non-regression test for https://github.com/dask/fastparquet/issues/778
def test_empty_valid_timestamp():
    df, views = empty(
        "i4",
        size=100,
        cols=["a"],
        index_types=["datetime64[ms]"],
        index_names=["timestamp"],
    )
    assert isinstance(df.index, pd.DatetimeIndex)


def test_timestamps():
    z = 'US/Eastern'

    # single column
    df, views = empty('M8', 100, cols=['t'])
    assert df.t.dt.tz is None
    views['t'].dtype.kind == "M"

    df, views = empty('M8', 100, cols=['t'], timezones={'t': z})
    assert str(df.t.dt.tz) == z
    views['t'].dtype.kind == "M"

    # one time column, one normal
    df, views = empty('M8,i', 100, cols=['t', 'i'], timezones={'t': z})
    assert str(df.t.dt.tz) == z
    views['t'].dtype.kind == "M"
    views['i'].dtype.kind == 'i'

    # no effect of timezones= on non-time column
    df, views = empty('M8,i', 100, cols=['t', 'i'], timezones={'t': z, 'i': z})
    assert str(df.t.dt.tz) == z
    assert df.i.dtype.kind == 'i'
    views['t'].dtype.kind == "M"
    views['i'].dtype.kind == 'i'

    # multi-timezones
    z2 = 'US/Central'
    df, views = empty('M8,M8', 100, cols=['t1', 't2'], timezones={'t1': z,
                                                                  't2': z})
    assert str(df.t1.dt.tz) == z
    assert str(df.t2.dt.tz) == z

    df, views = empty('M8,M8', 100, cols=['t1', 't2'], timezones={'t1': z})
    assert str(df.t1.dt.tz) == z
    assert df.t2.dt.tz is None

    df, views = empty('M8,M8', 100, cols=['t1', 't2'], timezones={'t1': z,
                                                                  't2': 'UTC'})
    assert str(df.t1.dt.tz) == z
    assert str(df.t2.dt.tz) == 'UTC'

    df, views = empty('M8,M8', 100, cols=['t1', 't2'], timezones={'t1': z,
                                                                  't2': z2})
    assert str(df.t1.dt.tz) == z
    assert str(df.t2.dt.tz) == z2


def test_pandas_hive_serialization(tmpdir):
    parquet_dir = tmpdir.join("test.par")
    column = "data"
    df = pd.DataFrame(
        columns=[column], data=[("42",), ("",), ("0",), ("1",), ("0.0",)]
    )
    df.to_parquet(parquet_dir, file_scheme="hive", row_group_offsets=[0, 2, 4], engine='fastparquet')

    df_ = pd.read_parquet(parquet_dir, engine='fastparquet')
    assert_frame_equal(df, df_)
