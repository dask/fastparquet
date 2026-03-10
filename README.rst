fastparquet
===========

.. image:: https://github.com/dask/fastparquet/actions/workflows/main.yaml/badge.svg
    :target: https://github.com/dask/fastparquet/actions/workflows/main.yaml

.. image:: https://readthedocs.org/projects/fastparquet/badge/?version=latest
    :target: https://fastparquet.readthedocs.io/en/latest/

fastparquet is a python implementation of the `parquet
format <https://github.com/apache/parquet-format>`_, aiming integrate
into python-based big data work-flows. It is used implicitly by
the projects Dask, Pandas and intake-parquet.

We offer a high degree of support for the features of the parquet format, and
very competitive performance, in a small install size and codebase.

Details of this project, how to use it and comparisons to other work can be found in the documentation_.

.. note::

   March 2026. The release of pandas 3.0 has broken a number of things in ``fastparquet``. Since pandas now
   depends explicitly on pyarrow, there is no longer any demand for the existence of this project, and it
   is being retired. Perhaps use will continue for those still using pandas 2.x, but we anticipate no
   further development.

.. _documentation: https://fastparquet.readthedocs.io

Requirements
------------

(all development is against recent versions in the default anaconda channels
and/or conda-forge)

Required:

- numpy
- pandas
- cython >= 0.29.23 (if building from pyx files)
- cramjam
- fsspec

Supported compression algorithms:

- Available by default:

  - gzip
  - snappy
  - brotli
  - lz4
  - zstandard

- Optionally supported
  
  - `lzo <https://github.com/jd-boyd/python-lzo>`_


Installation
------------

Install using conda, to get the latest compiled version::

   conda install -c conda-forge fastparquet

or install from PyPI::

   pip install fastparquet

You may wish to install numpy first, to help pip's resolver.
This may install an appropriate wheel, or compile from source. For the latter,
you will need a suitable C compiler toolchain on your system.

You can also install latest version from github::

   pip install git+https://github.com/dask/fastparquet

in which case you should also have ``cython`` to be able to rebuild the C files.

Usage
-----

Please refer to the documentation_.

*Reading*

.. code-block:: python

    from fastparquet import ParquetFile
    pf = ParquetFile('myfile.parq')
    df = pf.to_pandas()
    df2 = pf.to_pandas(['col1', 'col2'], categories=['col1'])

You may specify which columns to load, which of those to keep as categoricals
(if the data uses dictionary encoding). The file-path can be a single file,
a metadata file pointing to other data files, or a directory (tree) containing
data files. The latter is what is typically output by hive/spark.

*Writing*

.. code-block:: python

    from fastparquet import write
    write('outfile.parq', df)
    write('outfile2.parq', df, row_group_offsets=[0, 10000, 20000],
          compression='GZIP', file_scheme='hive')

The default is to produce a single output file with a single row-group
(i.e., logical segment) and no compression. At the moment, only simple
data-types and plain encoding are supported, so expect performance to be
similar to *numpy.savez*.

History
-------

This project forked in October 2016 from `parquet-python`_, which was not designed
for vectorised loading of big data or parallel access.

.. _parquet-python: https://github.com/jcrobak/parquet-python

