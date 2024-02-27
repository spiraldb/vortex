import pyarrow as pa
import pytest

import enc


def test_primitive_array_round_trip():
    a = pa.array([0, 1, 2, 3])
    arr = enc.encode(a)
    assert isinstance(arr, enc.PrimitiveArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_varbin_array_round_trip():
    a = pa.array(["a", "b", "c"])
    arr = enc.encode(a)
    assert isinstance(arr, enc.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


@pytest.mark.xfail(strict=True)
def test_varbin_array_doesnt_round_trip():
    a = pa.array(["a", "b", "c"], type=pa.large_utf8())
    arr = enc.encode(a)
    assert isinstance(arr, enc.VarBinArray)
    assert arr.to_pyarrow().combine_chunks() == a


def test_empty_array():
    a = pa.array([], type=pa.uint8())
    primitive = enc.encode(a)
    assert primitive.to_pyarrow().type == pa.uint8()
