Release Notes
=============

Releases since 2023 are essentially bugfixes only and updating
supported python versions.

2023.10.0
---------

#. Datetime units in empty() with tz (#893)
#. Fewer inplace decompressions for V2 pages (#890
#. Allow writing categorical column with no categories (#888)
#. Fixes for new numpy (#886)
#. RLE bools and DELTA for v1 pages (#885, 883)

2023.8.0
--------

#. More general timestamp units (#874)
#. ReadTheDocs V2 (#871)
#. Better roundtrip dtypes (#861, 859)
#. No convert when computing bytes-per-item for str (#858)


2023.4.0
--------

#. allow loading categoricals even if not so in the pandas metadata, when a column is dict-encoded
and we only have one row-group (#863)
#. apply dtype to the columns names series, even when selecting no columns (#861, 859)
#. don't make strings while estimating bye column size (#858)
#. handle upstream depr (#857, 856)


2023.2.0
--------

#. revert one-level set of filters (#852)
#. full size dict for decoding V2 pages (#850)
#. infer_object_encoding fix (#847)
#. row filtering with V2 pages (#845)

2023.1.0
--------

#. big improvement to write speed
#. paging support for bigger row-groups
#. pandas 2.0 support
#. delta for big-endian architecture

2022.12.0
---------

#. add py3.11 wheel builds
#. check all int32 values before passing to thrift writer
#. fix type of num_rows to i64 for big single file

2022.11.0
---------

#. Switch to calver
#. Speed up loading of nullable types
#. Allow schema evolution by addition of columns
#. Allow specifying dtypes of output
#. update to scm versioning
#. update CI and use mamba
#. fixes to row filter, statistics and tests
#. support pathlib.Paths
#. JSON encoder options

0.8.3
-----

#. improved key/value handling and rejection of bad types
#. fix regression in consolidate_cats (caught in dask tests)

0.8.2
-----

#. datetime indexes initialised to 0 to prevent overflow from randommemory
#. case from csv_to_parquet where stats exists but has not nulls entry
#. define len and bool for ParquetFile
#. maintain int types of optional data tha came from pandas
#. fix for delta encoding

0.8.1
-----

#. fix critical buffer overflow crash for large number of columns and long column names
#. metadata handling
#. thrift int32 for list
#. avoid error storing NaNs in column stats

0.8.0
-----

#. our own cythonic thrift implementation (drop thrift dependency)
#. more in-place dataset editing ad reordering
#. python 3.10 support
#. fixes for multi-index and pandas types

0.7.2
-----

#. Ability to remove row-groups in-place for multifile datasets
#. Accept pandas nullable Float type
#. allow empty strings and fix min/max when there is no data
#. make writing statistics optional
#. row selection in to_pandas()

0.7.1
-----

#. Back compile for older versions of numpy
#. Make pandas nullable types opt-out. The old behaviour (casting to float)
   is still available with ``ParquetFile(..., pandas_nulls=False)``.
#. Fix time field regression: IsAdjustedToUTC will be False when there is no
   timezone
#. Micro improvements to the speed of ParquetFile creation by using simple
   simple string ops instead of regex and regularising filenames once at
   the start. Effects datasets with many files.

.. _0.7.0:

0.7.0
-----

(July 2021)

This version institutes major, breaking changes, listed here, and incremental
fixes and additions.


#. Reading a directory without a ``_metadata`` summary file now works by providing
   only the directory, instead of a list of constituent files. This change also
   makes direct of use of fsspec filesystems, if given, to be able to load the
   footer metadata areas of the files concurrently, if the storage backend supports
   it, and not directly instantiating intermediate ParquetFile instances
#. row-level filtering of the data. Whereas previously, only full row-groups could be
   excluded on the basis of their parquet metadata statistics (if present), filtering
   can now be done within row-groups too. The syntax is the same as before, allowing
   for multiple column expressions to be combined with AND|OR, depending on the
   list structure. This mechanism requires two passes: one to load the columns needed
   to create the boolean mask, and another to load the columns actually needed in the
   output. This will not be faster, and may be slower, but in some cases can save
   significant memory footprint, if a small fraction of rows are considered good and
   the columns for the filter expression are not in the output. Not currently
   supported for reading with DataPageV2.
#. DELTA integer encoding (read-only): experimentally working, but we only have one
   test file to verify against, since it is not trivial to persuade Spark to
   produce files encoded this way. DELTA can be extremely compact a representation
   for slowly varying and/or monotonically increasing integers.
#. nanosecond resolution times: the new extended "logical" types system supports
   nanoseconds alongside the previous millis and micros. We now emit these for the
   default pandas time type, and produce full parquet schema including both "converted"
   and "logical" type information. Note that all output has ``isAdjustedToUTC=True``,
   i.e., these are timestamps rather than local time. The time-zone is stored in the
   metadata, as before, and will be successfully recreated only in fastparquet and (py)arrow.
   Otherwise, the times will appear to be UTC. For compatibility with Spark, you may
   still want to use ``times="int96"`` when writing.
#. DataPageV2 writing:   now we support both reading and writing. For writing,
   can be enabled with the environment variable FASTPARQUET_DATAPAGE_V2, or module
   global ``fastparquet.writer.DATAPAGE_VERSION`` and is off by default. It will become
   on by default in the future. In many cases, V2 will result in
   better read performance, because the data and page headers are encoded separately, so data
   can be directly read into the output without addition allocation/copies. This feature
   is considered experimental, but we believe it working well for most use cases (i.e.,
   our test suite) and should be readable by all modern parquet frameworks including
   arrow and spark.
#. pandas nullable types: pandas supports "masked" extension arrays for types that previously
   could not support NULL at all: ints and bools. Fastparquet used to cast such columns
   to float, so that we could represent NULLs as NaN; now we use the new(er) masked types
   by default. This means faster reading of such columns, as there is no conversion. If the
   metadata guarantees that there are no nulls, we still use the non-nullable variant *unless*
   the data was written with fastparquet/pyarrow, and the metadata indicates that the original
   datatype was nullable. We already handled writing of nullable columns.

0.6.0
-----

(May 2021)

This version institutes major, breaking changes, listed here, and incremental
fixes and additions.


NB: minor versions up to 0.6.3 fix build issues

#. replacement of the numba dependency with cythonized code. This also brought many
   performance improvements, by reducing memory copies in many places, and an overhaul
   of many parts of the code. Replacing numba by cython did not affect the performance
   of specific functions, but has made installation of fastparquet much simpler, for not needing
   the numba/LLVM stack, and imports faster, for not having to compile any code at runtime.
#. distribution as pip-installable wheels. Since we are cythonizing more, we want to
   make installation as simple as we can. So we now produce wheels.
#. using `cramjam`_ as the comp/decompression backend, instead of separate libraries
   for snappy, zstd, brotli... . This decreases the size and complexity of the install,
   guarantees the availability of codecs (cramjam is a required dependency, but with
   no dependencies of its own), and for the parquet read case, where we know the size
   of the original data, brings a handy speed-up.
#. implementation of DataPageV2: reading (see also 0.7.0 entry): this has been in the parquet
   spec for a long time, but
   only seen sporadic take-up until recently. Using standard reference files from the parquet
   project, we ensure correct reading of some V2-encoded files.
#. RLE_DICT: this one is more of a fix. The parquet spec renamed PLAIN_DICTIONARY, or
   perhaps renamed the previous definition. We now follow the new definitions for writing
   and support both for reading.
#. support custom key/value metadata on write and preserve this metadata on append or
   consolidate of many data files.

.. _cramjam: https://github.com/milesgranger/pyrus-cramjam


.. raw:: html

    <script data-goatcounter="https://fastparquet.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>