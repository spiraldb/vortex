import enc
import pyarrow as pa


def test_primitive_compress():
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = enc.compress(enc.encode(a))
    assert isinstance(arr_compressed, enc.REEArray)
    assert arr_compressed.to_pyarrow().combine_chunks() == a
