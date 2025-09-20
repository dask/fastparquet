# -*- coding: utf-8 -*-
"""test_converted_types.py - tests for decoding data to their logical data types."""
import datetime
import os.path
import zoneinfo

import numpy as np
import pandas as pd
import pytest
from packaging.version import Version

from fastparquet import parquet_thrift as pt
from fastparquet.converted_types import convert
from fastparquet.util import PANDAS_VERSION


def test_int32():
    """Test decimal data stored as int32."""
    schema = pt.SchemaElement(
        type=pt.Type.INT32,
        name="test",
        converted_type=pt.ConvertedType.DECIMAL,
        scale=10,
        precision=9
    )

    assert (convert(pd.Series([9876543210]), schema)[0] - 9.87654321) < 0.01


def test_date():
    """Test int32 encoding a date."""
    schema = pt.SchemaElement(
        type=pt.Type.INT32,
        name="test",
        converted_type=pt.ConvertedType.DATE,
    )
    days = (datetime.date(2004, 11, 3) - datetime.date(1970, 1, 1)).days
    data = pd.Series([days]).to_numpy()
    data.flags.writeable = False
    assert (convert(data, schema)[0] ==
            pd.to_datetime([datetime.date(2004, 11, 3)]))


def test_time_millis():
    """Test int32 encoding a timedelta in millis."""
    schema = pt.SchemaElement(
        type=pt.Type.INT32,
        name="test",
        converted_type=pt.ConvertedType.TIME_MILLIS,
    )
    assert (convert(np.array([731888], dtype='int32'), schema)[0] ==
            np.array([731888], dtype='timedelta64[ms]'))


def test_timestamp_millis():
    """Test int64 encoding a datetime."""
    schema = pt.SchemaElement(
        type=pt.Type.INT64,
        name="test",
        converted_type=pt.ConvertedType.TIMESTAMP_MILLIS,
    )
    assert (convert(np.array([1099511625014], dtype='int64'), schema)[0] ==
            np.array(datetime.datetime(2004, 11, 3, 19, 53, 45, 14 * 1000),
                dtype='datetime64[ns]'))


@pytest.mark.xfail(
                PANDAS_VERSION >= Version("3.dev"),
                reason=("Need to add pandas v3 support: "
                        "pandas tries to decode `data`, resulting in "
                        "AttributeError: 'str' object has no attribute 'decode'")
            )
def test_utf8():
    """Test bytes representing utf-8 string."""
    schema = pt.SchemaElement(
        type=pt.Type.BYTE_ARRAY,
        name="test",
        converted_type=pt.ConvertedType.UTF8
    )
    data = u"Ördög"  # conversion now happens on read
    assert convert(pd.Series([data]), schema)[0] == u"Ördög"


def test_json():
    """Test bytes representing json."""
    schema = pt.SchemaElement(
        type=pt.Type.BYTE_ARRAY,
        name="test",
        converted_type=pt.ConvertedType.JSON
    )
    assert convert(pd.Series([b'{"foo": ["bar", "\\ud83d\\udc7e"]}']),
                          schema)[0] == {'foo': ['bar', u'👾']}


def test_bson():
    """Test bytes representing bson."""
    bson = pytest.importorskip('bson')
    schema = pt.SchemaElement(
        type=pt.Type.BYTE_ARRAY,
        name="test",
        converted_type=pt.ConvertedType.BSON
    )
    assert convert(pd.Series(
            [b'&\x00\x00\x00\x04foo\x00\x1c\x00\x00\x00\x020'
             b'\x00\x04\x00\x00\x00bar\x00\x021\x00\x05\x00'
             b'\x00\x00\xf0\x9f\x91\xbe\x00\x00\x00']),
            schema)[0] == {'foo': ['bar', u'👾']}


def test_uint16():
    """Test decoding int32 as uint16."""
    schema = pt.SchemaElement(
        type=pt.Type.INT32,
        name="test",
        converted_type=pt.ConvertedType.UINT_16
    )
    assert convert(pd.Series([-3]), schema)[0] == 65533


def test_uint32():
    """Test decoding int32 as uint32."""
    schema = pt.SchemaElement(
        type=pt.Type.INT32,
        name="test",
        converted_type=pt.ConvertedType.UINT_32
    )
    assert convert(pd.Series([-6884376]), schema)[0] == 4288082920


def test_uint64():
    """Test decoding int64 as uint64."""
    schema = pt.SchemaElement(
        type=pt.Type.INT64,
        name="test",
        converted_type=pt.ConvertedType.UINT_64
    )
    assert convert(pd.Series([-6884376]), schema)[0] == 18446744073702667240


def test_big_decimal():
    schema = pt.SchemaElement(
        type=pt.Type.FIXED_LEN_BYTE_ARRAY,
        name="test",
        converted_type=pt.ConvertedType.DECIMAL,
        type_length=32,
        scale=1,
        precision=38
    )
    pad = b'\x00' * 16
    data = np.array([
    pad, pad + b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1e\\',
    pad + b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1d\\',
    pad + b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r{',
    pad + b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19)'],
            dtype='|S32')
    assert np.isclose(convert(data, schema),
                      np.array([0., 777.2, 751.6, 345.1, 644.1])).all()


def test_tz_nonstring(tmpdir):
    # https://github.com/dask/fastparquet/issues/578
    import uuid

    event = {}
    event_id = str(uuid.uuid4())
    event['id'] = [event_id]
    event['site_name'] = ['TestString']
    event['start_time'] = ['2021-01-01T14:58:19.3677-05:00']
    event['end_time'] = ['2021-01-01T14:59:50.5272-05:00']

    event_df = pd.DataFrame(event)
    event_df['start_time'] = pd.to_datetime(event_df['start_time'])
    event_df['end_time'] = pd.to_datetime(event_df['end_time'])
    fn = '{}/{}.parquet'.format(tmpdir, event_id)
    event_df.to_parquet(fn, compression='uncompressed', engine='fastparquet')

    round = pd.read_parquet(fn, engine="fastparquet")
    assert (event_df == round).all().all()


def test_tz_zoneinfo(tmpdir):
    dti = pd.DatetimeIndex([pd.Timestamp(2020, 1, 1)], name="a").tz_localize(zoneinfo.ZoneInfo("UTC"))
    df = pd.DataFrame({"a": dti})
    fn = '{}/{}.parquet'.format(tmpdir, 'zoneinfo_tmp')
    df.to_parquet(fn, compression='uncompressed', engine='fastparquet')
    result = pd.read_parquet(fn, engine="fastparquet")
    result_dtype = result.iloc[:, 0].dtype
    assert isinstance(result_dtype, pd.DatetimeTZDtype)
    assert str(result_dtype.tz) == "UTC"


def test_pandas_simple_type(tmpdir):
    import pandas as pd
    fn = os.path.join(tmpdir, "out.parquet")
    df = pd.DataFrame({"a": [1, 2, 3]}, dtype='uint8')
    df.to_parquet(fn, engine="fastparquet")
    df2 = pd.read_parquet(fn, engine="fastparquet")
    assert df2.a.dtype == "uint8"
    assert not(isinstance(df2.a.dtype, pd.UInt8Dtype))
