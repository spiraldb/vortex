import vortex


def test_int():
    assert str(vortex.int()) == "i64"
    assert str(vortex.int(32)) == "i32"
    assert str(vortex.int(32, nullable=True)) == "i32?"
    assert str(vortex.uint(32)) == "u32"
    assert str(vortex.float(16)) == "f16"
    assert str(vortex.bool(nullable=True)) == "bool?"
