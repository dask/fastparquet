fastparquet
===========

A Python interface to the Parquet file format.

Introduction
------------

The `Parquet format <https://github.com/apache/parquet-format>`_ is a common binary data store, used
particularly in the Hadoop/big-data sphere. It provides several advantages relevant to big-data
processing, including:

#. columnar storage, only read the data of interest
#. efficient binary packing
#. choice of compression algorithms and encoding
#. split data into files, allowing for parallel processing
#. range of logical types
#. statistics stored in metadata allow for skipping unneeded chunks
#. data partitioning using the directory structure

Since it was developed as part of the Hadoop ecosystem, Parquet's reference implementation is
written in Java. This package aims to provide a performant library to read and write Parquet files
from Python, without any need for a Python-Java bridge. This will make the Parquet format an
ideal storage mechanism for Python-based big data workflows.

The tabular nature of Parquet is a good fit for the Pandas data-frame objects, and
we exclusively deal with data-frame<->Parquet.

Highlights
----------

The original outline plan for this project can be found `upstream <https://github.com/dask/fastparquet/issues/1>`_

Briefly, some features of interest:

#. read and write Parquet files, in single
   or multiple-file format
#. choice of compression per-column and various optimized encoding schemes;
   ability to choose row divisions and partitioning on write.
#. acceleration of both reading and writing using ``cython``, competitive performance versus other
   frameworks
#. ability to read and write to arbitrary file-like objects,
   allowing interoperability with ``fsspec`` filesystems and others
#. can be called from `dask <http://dask.pydata.org>`_, to enable parallel reading and writing
   with Parquet files,
   possibly distributed across a cluster.

Caveats, Known Issues
---------------------

Please see the :doc:`releasenotes`. With versions 0.6.0 and 0.7.0, a LOT of new features
and enhancements were added, so please read that page carefully, this may affect you!

Fastparquet is a free and open-source project.
We welcome contributions in the form of bug reports, documentation, code, design proposals, and more.
This page provides resources on how best to contribute.

**Bug reports**

Please file an issue on `github <https://github.com/dask/fastparquet/>`_.


Relation to Other Projects
--------------------------

#. `parquet-python <https://github.com/jcrobak/parquet-python>`_ is the original
   pure-Python Parquet quick-look utility which was the inspiration for fastparquet.
   It has continued development, but is not directed as big data vectorised loading
   as we are.
#. Apache `Arrow <http://pyarrow.readthedocs.io/en/latest/>`_ and its python
   API define an in-memory data representation, and can read/write parquet, including
   conversion to pandas. It is the "other" engine available within Dask and Pandas, and
   gives good performance and a range of options. If you are using Arrow anyway, you probably
   want to use its parquet interface.
#. `PySpark <http://Spark.apache.org/docs/2.1.0/programming-guide.html>`_, a Python API to the Spark
   engine, interfaces Python commands with a Java/Scala execution core, and thereby
   gives Python programmers access to the Parquet format. Spark is used in some tests and
   some test files were produced by Spark.
#. fastparquet lives within the `dask <http://dask.pydata.org>`_ ecosystem, and
   although it is useful by itself, it is designed to work well with dask for parallel
   execution, as well as related libraries such as s3fs for pythonic access to
   Amazon S3.


Index
-----

.. toctree::

    install
    releasenotes
    quickstart
    details
    api
    filesystems

1. :ref:`genindex`
1. :ref:`modindex`
1. :ref:`search`


These docs pages collect anonymous tracking data using goatcounter, and the
dashboard is available to the public: https://fastparquet.goatcounter.com/ .



.. raw:: html

    <script data-goatcounter="https://fastparquet.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>