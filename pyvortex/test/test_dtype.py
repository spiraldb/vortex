import vortex


def test_int():
    assert str(vortex.int()) == "int(64)"
    assert str(vortex.int(32)) == "int(32)"
    assert str(vortex.int(32, nullable=True)) == "int(32)?"
    assert str(vortex.uint(32)) == "uint(32)"
    assert str(vortex.float(16)) == "float(16)"
    assert str(vortex.bool(nullable=True)) == "bool?"
