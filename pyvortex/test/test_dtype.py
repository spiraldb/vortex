import vortex


def test_int():
    assert str(vortex.dtype.int()) == "i64"
    assert str(vortex.dtype.int(32)) == "i32"
    assert str(vortex.dtype.int(32, nullable=True)) == "i32?"
    assert str(vortex.dtype.uint(32)) == "u32"
    assert str(vortex.dtype.float(16)) == "f16"
    assert str(vortex.dtype.bool(nullable=True)) == "bool?"
