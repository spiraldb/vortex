import enc
import numpy as np
import pyarrow as pa


def test_primitive_compress():
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = enc.compress(enc.encode(a))
    assert isinstance(arr_compressed, enc.REEArray)
    assert arr_compressed.to_pyarrow().combine_chunks() == a


def test_roaring_bool_compress():
    a = enc.encode(pa.array([True] * 10_000))
    rarr = enc.RoaringBoolArray.encode(a)
    assert isinstance(rarr, enc.RoaringBoolArray)
    assert rarr.nbytes < a.nbytes


def test_roaring_int_compress():
    a = enc.encode(pa.array(np.arange(10_000), type=pa.uint32()))
    compressed = enc.compress(a)
    assert compressed.encoding == "roaring.int"


def test_zigzag_compress():
    a = enc.encode(pa.array([-1, -1, 0, -1, 1, -1]))
    zarr = enc.ZigZagArray.encode(a)
    assert isinstance(zarr, enc.ZigZagArray)
    # TODO(ngates): support decoding once we have decompressor.
