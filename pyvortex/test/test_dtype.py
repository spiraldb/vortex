import vortex


def test_int():
    assert str(vortex.dtype.int()) == "int(64, False)"
    assert str(vortex.dtype.int(32)) == "int(32, False)"
    assert str(vortex.dtype.int(32, nullable=True)) == "int(32, True)"
    assert str(vortex.dtype.uint(32)) == "uint(32, False)"
    assert str(vortex.dtype.float(16)) == "float(16, False)"
    assert str(vortex.dtype.bool(nullable=True)) == "bool(True)"
