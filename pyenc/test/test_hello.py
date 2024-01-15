import enc
import pyarrow as pa


def test_array():
    a = pa.array([0, 1, 2, 3])
    primitive = enc.PrimitiveArray(a)
    assert primitive.to_pyarrow().combine_chunks() == a


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = enc.PrimitiveArray(a)
    assert primitive.to_pyarrow().type == pa.uint8()
