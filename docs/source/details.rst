Usage Notes
===========

Some additional information to bear in mind when using fastparquet,
in no particular order. Much of what follows has implications for writing
parquet files that are compatible with other parquet implementations, versus
performance when writing data for reading back with fastparquet.
Please also read the :doc:`releasenotes` for newer or experimental features.

Whilst we aim to make the package simple to use, some choices on the part
of the user may effect performance and data consistency.

Categoricals
------------

When writing a data-frame with a column of pandas type ``Category``, the
data will be encoded using Parquet "dictionary encoding". This stores all
the possible values of the column (typically strings) separately, and the
index corresponding to each value as a data set of integers. If there
is a significant performance gain to be made, such as long labels, but low
cardinality, users are suggested to turn their object columns into the
category type:

.. code-block:: python

    df[col] = df[col].astype('category')

Fastparquet will automatically use metadata information to load such columns
as categorical *if* the data was written by fastparquet/pyarrow.

To efficiently load a column as a categorical type for data from other
parquet frameworks, include it in the optional
keyword parameter ``categories``; however it must be encoded as dictionary
throughout the dataset, *with the same labels in every part*.

.. code-block:: python

    pf = ParquetFile('input.parq')
    df = pf.to_pandas(categories={'cat': 12})

Where we provide a hint that the column ``cat`` has up to 12 possible values.
``categories`` can also take a list, in which case up to 32767 (2**15 - 1)
labels are assumed.
For data not written by fastparquet/pyarrow, columns that are encoded as dictionary
but not included in ``categories`` will
be de-referenced on load, which is potentially expensive.

Byte Arrays
-----------

Fixed-length byte arrays provide a modest speed boost for binary data (bytestrings)
whose lengths really are all the same or nearly so. To automatically
convert string values to fixed-length when writing, use the ``fixed_text``
optional keyword, with a predetermined length.

.. code-block:: python

    write('out.parq', df, fixed_text={'char_code': 1})

This is not recommended for strings, since UTF8 encoding/decoding must be done anyway,
and converting to fixed will probably just waste time.

Nulls
-----

In pandas, there is no internal representation difference between NULL (no value)
and NaN/NaT (not a valid number) for float and time columns. Other parquet frameworks
(e.g., in `in Spark`_) might likely treat
NULL and NaN differently. In the typical case of tabular
data (as opposed to strict numerics), users often mean the NULL semantics, and
so should write NULLs information. Furthermore, it is typical for some parquet
frameworks to define all columns as optional, whether or not they are intended to
hold any missing data, to allow for possible mutation of the schema when appending
partitions later.

.. _in Spark: https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#nan-semantics

Since there is some cost associated with reading and writing NULLs information,
fastparquet provides the ``has_nulls`` keyword when writing to specify how to
handle NULLs. In the case that a column has no NULLs, including NULLs information
will not produce a great performance hit on reading, and only a slight extra time
upon writing, while determining that there are zero NULL values.

The following cases are allowed for ``has_nulls``:

#. True: all columns become optional, and NaNs are always stored as NULL. This is
   the best option for compatibility. This is the default.
#. False: all columns become required, and any NaNs are stored as NaN; if there
   are any fields which cannot store such sentinel values (e.g,. string),
   but do contain None, there will be an error.
#. 'infer': only object columns will become optional, since float, time, and
   category columns can store sentinel values, and ordinary pandas int columns cannot
   contain any NaNs. This is the best-performing
   option if the data will only be read by fastparquet. Pandas nullable columns
   will be stored as optional, whether or not they contain nulls.
#. list of strings: the named columns will be optional, others required (no NULLs)

Data Types
----------

There is fairly good correspondence between pandas data-types and Parquet
simple and logical data types.
The `types documentation <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>`_
gives details of the implementation spec.

A couple of caveats should be noted:

#. fastparquet will
   not write any Decimal columns, only float, and when reading such columns,
   the output will also be float, with potential machine-precision errors;
#. only UTF8 encoding for text is automatically handled, although arbitrary
   byte strings can be written as raw bytes type;
#. all times are stored as UTC, but the timezone is stored in the metadata, so
   will be recreated if loaded into pandas

Reading Nested Schema
---------------------

Fastparquet can read nested schemas. The principal mechamism is *flattening*, whereby
parquet schema struct columns become top-level columns. For instance, if a schema looks
like

.. code-block:: python

    root
    | - visitor: OPTIONAL
      | - ip: BYTE_ARRAY, UTF8, OPTIONAL
        - network_id: BYTE_ARRAY, UTF8, OPTIONAL

then the ``ParquetFile`` will include entries "visitor.ip" and "visitor.network_id" in its
``columns``, and these will become ordinary Pandas columns. We do not generate a hierarchical
column index.

Fastparquet also handles some parquet LIST and MAP types. For instance, the schema may include

.. code-block:: python

    | - tags: LIST, OPTIONAL
        - list: REPEATED
           - element: BYTE_ARRAY, UTF8, OPTIONAL

In this case, ``columns`` would include an entry "tags", which evaluates to an object column
containing lists of strings. Reading such columns will be relatively slow.
If the 'element' type is anything other than a primitive type,
i.e., a struct, map or list, than fastparquet will not be able to read it, and the resulting
column will either not be contained in the output, or contain only ``None`` values.

Object encoding
---------------

Object columns are Json encoded and decoded if the data type is ``json``,
as specified by the ``object_encoding`` parameter in ``fastparquet.write``.

If any of the libraries ``orjson``, ``ujson``, ``python-rapidjson`` is installed,
then it's used in place of the ``json`` module in the Python Standard Library.

Using one of these libraries may improve both the reading and writing performance.

You can also enforce the use of a specific library by setting the environment variable
``FASTPARQUET_JSON_CODEC`` to one of the supported modules:

* ``orjson``
* ``ujson``
* ``rapidjson``
* ``json``

Partitions and row-groups
-------------------------

The Parquet format allows for partitioning the data by the values of some
(low-cardinality) columns and by row sequence number. Both of these can be
in operation at the same time, and, in situations where only certain sections
of the data need to be loaded, can produce great performance benefits in
combination with load filters.

Splitting on both row-groups and partitions can potentially result in many
data-files and large metadata. It should be used sparingly, when partial
selecting of the data is anticipated.

**Row groups**

The keyword parameter ``row_group_offsets`` allows control of the row
sequence-wise splits in the data. For example, with the default value,
each row group will contain 50 million rows. The exact index of the start
of each row-group can also be specified, which may be appropriate in the
presence of a monotonic index: such as a time index might lead to the desire
to have all the row-group boundaries coincide with year boundaries in the
data.

**Partitions**

In the presence of some low-cardinality columns, it may be advantageous to
split data data on the values of those columns. This is done by writing a
directory structure with *key=value* names. Multiple partition columns can
be chosen, leading to a multi-level directory tree.

Consider the following directory tree from this `Spark example <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery>`_:

    table/
        gender=male/
           country=US/
              data.parquet
           country=CN/
              data.parquet
        gender=female/
            country=US/
               data.parquet
            country=CN/
               data.parquet

Here the two partitioned fields are *gender* and *country*, each of which have
two possible values, resulting in four datafiles. The corresponding columns
are not stored in the data-files, but inferred on load, so space is saved,
and if selecting based on these values, potentially some of the data need
not be loaded at all.

If there were two row groups and the same partitions as above, each leaf
directory would contain (up to) two files, for a total of eight. If a
row-group happens to contain no data for one of the field value combinations,
that data file is omitted.


Iteration
---------

For data-sets too big to fit conveniently into memory, it is possible to
iterate through the row-groups in a similar way to reading by chunks from
CSV with pandas.

.. code-block:: python

    pf = ParquetFile('myfile.parq')
    for df in pf.iter_row_groups():
        print(df.shape)
        # process sub-data-frame df

Thus only one row-group is in memory at a time. The same set of options
are available as in ``to_pandas`` allowing, for instance, reading only
specific columns, loading to
categoricals or to ignore some row-groups using filtering.

To get the first row-group only, one would go:

.. code-block:: python

    first = next(iter(pf.iter_row_groups()))

You can also grab the first N rows of the first row-group with :func:`fastparquet.ParquetFile.head`,
or select from among a data-set's row-groups using slice notation ``pf_subset = pf[2:8]``.

Dask/Pandas
-----------

Dask and Pandas fully support calling ``fastparquet`` directly, with the function
``read_parquet`` and method ``to_parquet``, specifying ``engine="fastparquet"``.
Please see their relevant docstrings. Remote filesystems are supported by using
a URL with a "protocol://" specifier and any ``storage_options`` to be passed to
the file system implementation.


.. raw:: html

    <script data-goatcounter="https://fastparquet.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
