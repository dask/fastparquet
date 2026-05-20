"""parquet - read parquet files."""

try:
    from fastparquet._version import __version__
except ImportError:
    __version__ = "0.0.0"

from fastparquet.writer import write, update_file_custom_metadata
from fastparquet import core, schema, converted_types, api
from fastparquet.api import ParquetFile
from fastparquet.util import ParquetException
