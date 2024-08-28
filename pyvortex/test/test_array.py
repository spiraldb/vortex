import pyarrow as pa
import vortex


def test_primitive_array_round_trip():
    a = pa.array([0, 1, 2, 3])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.PrimitiveArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_array_with_nulls():
    a = pa.array([b'123', None])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_varbin_array_round_trip():
    a = pa.array(["a", "b", "c"])
    arr = vortex.encode(a)
    assert isinstance(arr, vortex.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_varbin_array_take():
    a = vortex.encode(pa.array(["a", "b", "c", "d"]))
    assert a.take(vortex.encode(pa.array([0, 2]))).to_pyarrow().combine_chunks() == pa.array(
        ["a", "c"],
        type=pa.utf8(),
    )


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = vortex.encode(a)
    assert primitive.to_pyarrow().type == pa.uint8()
