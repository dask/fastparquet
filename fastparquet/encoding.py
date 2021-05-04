"""encoding.py - methods for reading parquet encoded data blocks."""
import array

import numpy as np
from .speedups import unpack_byte_array
from .thrift_structures import parquet_thrift


def read_plain_boolean(raw_bytes, count):
    data = np.frombuffer(raw_bytes, dtype='uint8')
    out = np.unpackbits(data).astype(bool, copy=False).reshape(-1, 8)[:, ::-1].reshape(-1)
    return out[:count]


DECODE_TYPEMAP = {
    parquet_thrift.Type.INT32: np.int32,
    parquet_thrift.Type.INT64: np.int64,
    parquet_thrift.Type.INT96: np.dtype('S12'),
    parquet_thrift.Type.FLOAT: np.float32,
    parquet_thrift.Type.DOUBLE: np.float64,
}


def read_plain(raw_bytes, type_, count, width=0, utf=False, stat=False):
    if type_ in DECODE_TYPEMAP:
        dtype = DECODE_TYPEMAP[type_]
        return np.frombuffer(memoryview(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
        if count == 1:
            width = len(raw_bytes)
        dtype = np.dtype('S%i' % width)
        return np.frombuffer(memoryview(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.BOOLEAN:
        return read_plain_boolean(raw_bytes, count)
    if type_ == parquet_thrift.Type.BYTE_ARRAY:
        if stat:
            if utf:
                return np.array([bytes(raw_bytes).decode()], dtype='O')
            else:
                return np.array([bytes(raw_bytes)], dtype='O')
        return np.array(unpack_byte_array(raw_bytes, count, utf=utf))
