import numpy as np
import pyarrow as pa

import enc


def test_primitive_compress():
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = enc.compress(enc.encode(a))
    assert not isinstance(arr_compressed, enc.PrimitiveArray)
    assert arr_compressed.nbytes < a.nbytes


def test_bool_compress():
    a = enc.encode(pa.array([False] * 10_000 + [True] * 10_000))
    arr_compressed = enc.compress(a)
    assert len(arr_compressed) == 20_000
    assert isinstance(arr_compressed, enc.RoaringBoolArray)
    assert arr_compressed.nbytes < a.nbytes


def test_roaring_bool_encode():
    a = enc.encode(pa.array([True] * 10_000))
    rarr = enc.RoaringBoolArray.encode(a)
    assert isinstance(rarr, enc.RoaringBoolArray)
    assert rarr.nbytes < a.nbytes


def test_roaring_int_encode():
    a = enc.encode(pa.array(np.arange(10_000), type=pa.uint32()))
    compressed = enc.compress(a)
    assert compressed.encoding == "roaring.int"


def test_zigzag_encode():
    a = enc.encode(pa.array([-1, -1, 0, -1, 1, -1]))
    zarr = enc.ZigZagArray.encode(a)
    assert isinstance(zarr, enc.ZigZagArray)
    # TODO(ngates): support decoding once we have decompressor.


def test_chunked_encode():
    chunked = pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])])
    encoded = enc.encode(chunked)
    assert isinstance(encoded, enc.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.array([0, 1, 2, 3, 4, 5])


def test_table_encode():
    table = pa.table(
        {
            "number": pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])]),
            "string": pa.chunked_array([pa.array(["a", "b", "c"]), pa.array(["d", "e", "f"])]),
        }
    )
    encoded = enc.encode(table)
    assert isinstance(encoded, enc.ChunkedArray)
    assert encoded.to_pyarrow().combine_chunks() == pa.StructArray.from_arrays(
        [pa.array([0, 1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e", "f"])], names=["number", "string"]
    )
