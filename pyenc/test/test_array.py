import enc
import pyarrow as pa


def test_primitive_array_round_trip():
    a = pa.array([0, 1, 2, 3])
    arr = enc.encode(a)
    assert isinstance(arr, enc.PrimitiveArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_varbin_array_round_trip():
    a = pa.array(["a", "b", "c"])
    arr = enc.encode(a)
    assert isinstance(arr, enc.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a.cast(pa.large_utf8())


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = enc.encode(a)
    assert primitive.to_pyarrow().type == pa.uint8()
