import warnings
import numpy as np
import pandas as pd
try:
    from thrift.protocol.TCompactProtocol import TCompactProtocolAccelerated as TCompactProtocol
except ImportError:
    from thrift.protocol.TCompactProtocol import TCompactProtocol

from . import encoding
from . encoding import read_plain
import fastparquet.cencoding as encoding
from .compression import decompress_data, rev_map, decom_into
from .converted_types import convert, typemap, converts_inplace
from .schema import _is_list_like, _is_map_like
from .speedups import unpack_byte_array
from .thrift_structures import parquet_thrift, read_thrift
from .util import val_to_num, ex_from_sep


def _read_page(file_obj, page_header, column_metadata):
    """Read the data page from the given file-object and convert it to raw,
    uncompressed bytes (if necessary)."""
    raw_bytes = file_obj.read(page_header.compressed_page_size)
    raw_bytes = decompress_data(
        raw_bytes,
        page_header.uncompressed_page_size,
        column_metadata.codec,
    )

    assert len(raw_bytes) == page_header.uncompressed_page_size, \
        "found {0} raw bytes (expected {1})".format(
            len(raw_bytes),
            page_header.uncompressed_page_size)
    return raw_bytes


def read_data(fobj, coding, count, bit_width, out=None):
    """For definition and repetition levels

    Reads with RLE/bitpacked hybrid, where length is given by first byte.

    out: potentially provide a len(count) uint8 array to reuse
    """
    out = out or np.empty(count, dtype=np.uint8)
    o = encoding.NumpyIO(out)
    if coding == parquet_thrift.Encoding.RLE:
        while o.tell() < count:
            encoding.read_rle_bit_packed_hybrid(fobj, bit_width, 0, o, itemsize=1)
    else:
        raise NotImplementedError('Encoding %s' % coding)
    return out


def read_def(io_obj, daph, helper, metadata):
    """
    Read the definition levels from this page, if any.
    """
    definition_levels = None
    num_nulls = 0
    if not helper.is_required(metadata.path_in_schema):
        max_definition_level = helper.max_definition_level(
            metadata.path_in_schema)
        bit_width = encoding.width_from_max_int(max_definition_level)
        if bit_width:
            # NB: num_values is index 1 for either type of page header
            definition_levels = read_data(
                    io_obj, parquet_thrift.Encoding.RLE,
                    daph.num_values, bit_width)
            num_nulls = daph.num_values - (definition_levels ==
                                           max_definition_level).sum()
        if num_nulls == 0:
            definition_levels = None
    return definition_levels, num_nulls


def read_rep(io_obj, daph, helper, metadata):
    """
    Read the repetition levels from this page, if any.
    """
    repetition_levels = None
    if len(metadata.path_in_schema) > 1:
        max_repetition_level = helper.max_repetition_level(
            metadata.path_in_schema)
        if max_repetition_level == 0:
            repetition_levels = None
        else:
            bit_width = encoding.width_from_max_int(max_repetition_level)
            # NB: num_values is index 1 for either type of page header
            repetition_levels = read_data(io_obj, parquet_thrift.Encoding.RLE,
                                          daph.num_values,
                                          bit_width)
    return repetition_levels


def read_data_page(f, helper, header, metadata, skip_nulls=False,
                   selfmade=False):
    """Read a data page: definitions, repetitions, values (in order)

    Only values are guaranteed to exist, e.g., for a top-level, required
    field.
    """
    daph = header.data_page_header
    raw_bytes = _read_page(f, header, metadata)
    io_obj = encoding.NumpyIO(raw_bytes)

    repetition_levels = read_rep(io_obj, daph, helper, metadata)

    if skip_nulls and not helper.is_required(metadata.path_in_schema):
        num_nulls = 0
        definition_levels = None
        skip_definition_bytes(io_obj, daph.num_values)
    else:
        definition_levels, num_nulls = read_def(io_obj, daph, helper, metadata)

    nval = daph.num_values - num_nulls
    se = helper.schema_element(metadata.path_in_schema)
    if daph.encoding == parquet_thrift.Encoding.PLAIN:

        width = helper.schema_element(metadata.path_in_schema).type_length
        values = read_plain(io_obj.read(),
                            metadata.type,
                            int(daph.num_values - num_nulls),
                            width=width,
                            utf=se.converted_type == 0)
    elif daph.encoding in [parquet_thrift.Encoding.PLAIN_DICTIONARY,
                           parquet_thrift.Encoding.RLE_DICTIONARY,
                           parquet_thrift.Encoding.RLE]:
        # bit_width is stored as single byte.
        if daph.encoding == parquet_thrift.Encoding.RLE:
            bit_width = se.type_length
        else:
            bit_width = io_obj.read_byte()
        if bit_width in [8, 16, 32] and selfmade:
            num = (encoding.read_unsigned_var_int(io_obj) >> 1) * 8
            values = np.frombuffer(io_obj.read(num * bit_width // 8),
                                   dtype='int%i' % bit_width)
        elif bit_width:
            if bit_width > 8:
                values = np.empty(daph.num_values-num_nulls+7, dtype=np.int32)
                o = encoding.NumpyIO(values.view('uint8'))
                encoding.read_rle_bit_packed_hybrid(
                            io_obj, bit_width, io_obj.len-io_obj.tell(), o=o, itemsize=4)
            else:
                values = np.empty(daph.num_values-num_nulls+7, dtype=np.uint8)
                o = encoding.NumpyIO(values)
                encoding.read_rle_bit_packed_hybrid(
                    io_obj, bit_width, io_obj.len-io_obj.tell(), o=o, itemsize=1)
            values = values.data[:nval]
        else:
            values = np.zeros(nval, dtype=np.int8)
    else:
        raise NotImplementedError('Encoding %s' % daph.encoding)
    return definition_levels, repetition_levels, values[:nval]


def skip_definition_bytes(io_obj, num):
    io_obj.seek(6, 1)
    n = num // 64
    while n:
        io_obj.seek(1, 1)
        n //= 128


def read_dictionary_page(file_obj, schema_helper, page_header, column_metadata, utf=False):
    """Read a page containing dictionary data.

    Consumes data using the plain encoding and returns an array of values.
    """
    raw_bytes = _read_page(file_obj, page_header, column_metadata)
    if column_metadata.type == parquet_thrift.Type.BYTE_ARRAY:
        # TODO: copies raw_bytes and also copies array (use copy=False)
        values = np.array(unpack_byte_array(raw_bytes,
                          page_header.dictionary_page_header.num_values, utf=utf), dtype='object')
    else:
        width = schema_helper.schema_element(
            column_metadata.path_in_schema).type_length
        values = read_plain(
                raw_bytes, column_metadata.type,
                page_header.dictionary_page_header.num_values, width)
    return values


def read_data_page_v2(infile, schema_helper, se, data_header2, cmd,
                      dic, assign, num, use_cat):
    """
    :param infile: open file
    :param schema_helper:
    :param se: schema element
    :param data_header2: page header struct
    :param cmd: column metadata
    :param dic: any dictionary labels encountered
    :param assign: output array (all of it)
    :param num: offset, rows so far
    :param use_cat: output is categorical?
    :return: None
    (1, TType.I32, 'num_values', None, None, ),  # 1
    (2, TType.I32, 'num_nulls', None, None, ),  # 2
    (3, TType.I32, 'num_rows', None, None, ),  # 3
    (4, TType.I32, 'encoding', None, None, ),  # 4
    (5, TType.I32, 'definition_levels_byte_length', None, None, ),  # 5
    (6, TType.I32, 'repetition_levels_byte_length', None, None, ),  # 6
    (7, TType.BOOL, 'is_compressed', None, True, ),  # 7
    (8, TType.STRUCT, 'statistics', [Statistics, None], None, ),  # 8

    """
    if data_header2.encoding not in [parquet_thrift.Encoding.PLAIN_DICTIONARY,
                                     parquet_thrift.Encoding.RLE_DICTIONARY,
                                     parquet_thrift.Encoding.PLAIN]:
        raise NotImplementedError
    max_rep = schema_helper.max_repetition_level(cmd.path_in_schema)
    max_def = schema_helper.max_definition_level(cmd.path_in_schema)
    # special case for UNCOMPRESSED
    # flag to see if we can use decompress_into
    into0 = ((use_cat or converts_inplace(se)) and data_header2.num_nulls == 0
              and max_rep == 0)
    into = (data_header2.is_compressed and rev_map[cmd.codec] in decom_into
            and into0)
    # TODO: only easy path
    assert max_def == max_rep == 0
    # cases
    # - can PLAIN decompress_into the output (may still convert)
    # - can read_into output if not compressed and PLAIN
    # - can LRE-read into output
    if into and into0:
        decomp = decom_into[rev_map[cmd.codec]]
        infile.seek(data_header2.definition_levels_byte_length +
                    data_header2.repetition_levels_byte_length, 1)
        decomp()


def read_col(column, schema_helper, infile, use_cat=False,
             selfmade=False, assign=None, catdef=None):
    """Using the given metadata, read one column in one row-group.

    Parameters
    ----------
    column: thrift structure
        Details on the column
    schema_helper: schema.SchemaHelper
        Based on the schema for this parquet data
    infile: open file or string
        If a string, will open; if an open object, will use as-is
    use_cat: bool (False)
        If this column is encoded throughout with dict encoding, give back
        a pandas categorical column; otherwise, decode to values
    """
    cmd = column.meta_data
    se = schema_helper.schema_element(cmd.path_in_schema)
    off = min((cmd.dictionary_page_offset or cmd.data_page_offset,
               cmd.data_page_offset))

    infile.seek(off)
    rows = cmd.num_values

    if use_cat:
        my_nan = -1
    else:
        if assign.dtype.kind in ['f', 'i', 'u']:
            my_nan = np.nan
        elif assign.dtype.kind in ["M", 'm']:
            # GH#489 use a NaT representation compatible with ExtensionArray
            my_nan = assign.dtype.type("NaT")
        else:
            my_nan = None

    num = 0
    row_idx = 0
    dic = None

    while num < rows:

        ph = read_thrift(infile, parquet_thrift.PageHeader)
        if ph.type == parquet_thrift.PageType.DICTIONARY_PAGE:
            dic2 = read_dictionary_page(infile, schema_helper, ph, cmd, utf=se.converted_type == 0)
            dic2 = convert(dic2, se)
            if use_cat and dic is not None and (dic2 != dic).any():
                raise RuntimeError("Attempt to read as categorical a column"
                                   "with multiple dictionary pages.")
            dic = dic2
            if use_cat and dic is not None:
                # fastpath skips the check the number of categories hasn't changed.
                # In this case, they may change, if the default RangeIndex was used.
                catdef._set_categories(pd.Index(dic), fastpath=True)
                if np.iinfo(assign.dtype).max < len(dic):
                    raise RuntimeError('Assigned array dtype (%s) cannot accommodate '
                                       'number of category labels (%i)' %
                                       (assign.dtype, len(dic)))
            continue
        if ph.type == parquet_thrift.PageType.DATA_PAGE_V2:
            read_data_page_v2(infile, schema_helper, se, ph.data_page_header_v2, cmd,
                              dic, assign, num, use_cat)
            continue
        if (selfmade and hasattr(cmd, 'statistics') and
                getattr(cmd.statistics, 'null_count', 1) == 0):
            skip_nulls = True
        else:
            skip_nulls = False
        defi, rep, val = read_data_page(infile, schema_helper, ph, cmd,
                                        skip_nulls, selfmade=selfmade)
        if rep is not None and assign.dtype.kind != 'O':  # pragma: no cover
            # this should never get called
            raise ValueError('Column contains repeated value, must use object '
                             'type, but has assumed type: %s' % assign.dtype)
        d = ph.data_page_header.encoding in [parquet_thrift.Encoding.PLAIN_DICTIONARY,
                                             parquet_thrift.Encoding.RLE_DICTIONARY]
        if use_cat and not d:
            if not hasattr(catdef, '_set_categories'):
                raise ValueError('Returning category type requires all chunks'
                                 ' to use dictionary encoding; column: %s',
                                 cmd.path_in_schema)

        max_defi = schema_helper.max_definition_level(cmd.path_in_schema)
        if rep is not None:
            null = not schema_helper.is_required(cmd.path_in_schema[0])
            null_val = (se.repetition_type !=
                        parquet_thrift.FieldRepetitionType.REQUIRED)
            row_idx = 1 + encoding._assemble_objects(assign, defi, rep, val, dic, d,
                                                     null, null_val, max_defi, row_idx)
        elif defi is not None:
            # TODO: if output is NULLABLE (e.g., IntegerArray) can use
            #  fastpath here, but need nulls array
            part = assign[num:num+len(defi)]
            part[defi != max_defi] = my_nan
            if d and not use_cat:
                part[defi == max_defi] = dic[val]
            elif not use_cat:
                part[defi == max_defi] = convert(val, se)
            else:
                part[defi == max_defi] = val
        else:
            # TODO: can use, fastpath here, may need nulls array if NULLABLE
            piece = assign[num:num+len(val)]
            if use_cat and not d:
                # only possible for multi-index
                warnings.warn("Non-categorical multi-index is likely brittle")
                val = convert(val, se)
                try:
                    i = pd.Categorical(val)
                except:
                    i = pd.Categorical(val.tolist())
                catdef._set_categories(pd.Index(i.categories), fastpath=True)
                piece[:] = i.codes
            elif d and not use_cat:
                piece[:] = dic[val]
            elif not use_cat:
                piece[:] = convert(val, se)
            else:
                piece[:] = val

        num += len(defi) if defi is not None else len(val)


def read_row_group_file(fn, rg, columns, categories, schema_helper, cats,
                        open=open, selfmade=False, index=None, assign=None,
                        scheme='hive', partition_meta=None):
    with open(fn, mode='rb') as f:
        return read_row_group(f, rg, columns, categories, schema_helper, cats,
                              selfmade=selfmade, index=index, assign=assign,
                              scheme=scheme, partition_meta=partition_meta)


def read_row_group_arrays(file, rg, columns, categories, schema_helper, cats,
                          selfmade=False, assign=None):
    """
    Read a row group and return as a dict of arrays

    Note that categorical columns (if appearing in the parameter categories)
    will be pandas Categorical objects: the codes and the category labels
    are arrays.
    """
    out = assign
    maps = {}

    for column in rg.columns:
        if (_is_list_like(schema_helper, column) or
                _is_map_like(schema_helper, column)):
            name = ".".join(column.meta_data.path_in_schema[:-2])
        else:
            name = ".".join(column.meta_data.path_in_schema)
        if name not in columns:
            continue

        read_col(column, schema_helper, file, use_cat=name+'-catdef' in out,
                 selfmade=selfmade, assign=out[name],
                 catdef=out.get(name+'-catdef', None))

        if _is_map_like(schema_helper, column):
            # TODO: could be done in fast loop in _assemble_objects?
            if name not in maps:
                maps[name] = out[name].copy()
            else:
                if column.meta_data.path_in_schema[0] == 'key':
                    key, value = out[name], maps[name]
                else:
                    value, key = out[name], maps[name]
                out[name][:] = [dict(zip(k, v)) if k is not None else None
                                for k, v in zip(key, value)]


def read_row_group(file, rg, columns, categories, schema_helper, cats,
                   selfmade=False, index=None, assign=None,
                   scheme='hive', partition_meta=None):
    """
    Access row-group in a file and read some columns into a data-frame.
    """
    partition_meta = partition_meta or {}
    if assign is None:
        raise RuntimeError('Going with pre-allocation!')
    read_row_group_arrays(file, rg, columns, categories, schema_helper,
                          cats, selfmade, assign=assign)

    for cat in cats:
        if scheme == 'hive':
            s = ex_from_sep('/')
            partitions = s.findall(rg.columns[0].file_path)
        else:
            partitions = [('dir%i' % i, v) for (i, v) in enumerate(
                rg.columns[0].file_path.split('/')[:-1])]
        key, val = [p for p in partitions if p[0] == cat][0]
        val = val_to_num(val, meta=partition_meta.get(key))
        assign[cat][:] = cats[cat].index(val)
