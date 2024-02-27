import vortex


def test_int():
    assert str(vortex.int()) == "signed_int(_)"
    assert str(vortex.int(32)) == "signed_int(32)"
    assert str(vortex.int(32, signed=False)) == "unsigned_int(32)"
    assert str(vortex.float(16)) == "float(16)"
    assert str(vortex.bool(nullable=True)) == "bool?"
