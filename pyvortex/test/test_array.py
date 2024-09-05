import pyarrow as pa
import vortex


def test_primitive_array_round_trip():
    a = pa.array([0, 1, 2, 3])
    arr = vortex.array(a)
    assert arr.to_arrow() == a


def test_array_with_nulls():
    a = pa.array([b"123", None])
    arr = vortex.array(a)
    assert arr.to_arrow() == a


def test_varbin_array_round_trip():
    a = pa.array(["a", "b", "c"])
    arr = vortex.array(a)
    assert arr.to_arrow() == a


def test_varbin_array_take():
    a = vortex.array(pa.array(["a", "b", "c", "d"]))
    assert a.take(vortex.array(pa.array([0, 2]))).to_arrow() == pa.array(
        ["a", "c"],
        type=pa.utf8(),
    )


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = vortex.array(a)
    assert primitive.to_arrow().type == pa.uint8()
