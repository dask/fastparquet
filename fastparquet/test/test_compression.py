from fastparquet.compression import (compress_data, decompress_data,
        compressions, decompressions)

import pytest


@pytest.mark.parametrize('fmt', compressions)
def test_compress_decompress_roundtrip(fmt):
    data = b'123' * 1000
    compressed = compress_data(data, compression=fmt)
    if fmt.lower() == 'uncompressed':
        assert compressed is data
    else:
        assert len(compressed) < len(data)

    decompressed = bytes(decompress_data(compressed, len(data), algorithm=fmt))
    assert data == decompressed


def test_compress_decompress_roundtrip_args_gzip():
    data = b'123' * 1000
    compressed = compress_data(
        data,
        compression={
            "type": "gzip",
            "args": {
                "compresslevel": 5,
            }
        }
    )
    assert len(compressed) < len(data)

    decompressed = bytes(decompress_data(compressed, len(data), algorithm="gzip"))
    assert data == decompressed


def test_compress_decompress_roundtrip_args_lz4():
    pytest.importorskip('lz4')
    data = b'123' * 1000
    compressed = compress_data(
        data,
        compression={
            "type": "lz4",
            "args": {
                "compression": 5,
                "store_size": False,
            }
        }
    )
    assert len(compressed) < len(data)

    decompressed = decompress_data(compressed, len(data), algorithm="lz4")
    assert data == bytes(decompressed)


def test_compress_decompress_roundtrip_args_zstd():
    data = b'123' * 1000
    compressed = compress_data(
        data,
        compression={
            "type": "zstd",
            "args": {
                "level": 5,
            }
        }
    )
    assert len(compressed) < len(data)

    decompressed = bytes(decompress_data(compressed, len(data), algorithm="zstd"))
    assert data == decompressed

def test_errors():
    with pytest.raises(RuntimeError) as e:
        compress_data(b'123', compression='not-an-algorithm')

    assert 'not-an-algorithm' in str(e.value)
    assert 'gzip' in str(e.value).lower()


def test_not_installed():
    compressions.pop('BROTLI', None)
    with pytest.raises(RuntimeError) as e:
        compress_data(b'123', compression=4)
    assert 'brotli' in str(e.value).lower()


def test_import_without_cramjam_gzip(monkeypatch):
    # some distributions build cramjam without gzip; importing fastparquet must still work
    import importlib
    import cramjam
    from fastparquet import compression

    monkeypatch.delattr(cramjam, "gzip")
    try:
        importlib.reload(compression)
        assert "GZIP" not in compression.compressions
        assert "GZIP" not in compression.decompressions
        assert "SNAPPY" in compression.compressions
        with pytest.raises(RuntimeError, match="not available"):
            compression.compress_data(b"123", compression="gzip")
    finally:
        monkeypatch.undo()
        importlib.reload(compression)
