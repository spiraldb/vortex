import enc


def test_int():
    assert str(enc.int()) == "sint(_)"
    assert str(enc.int(32)) == "sint(32)"
    assert str(enc.int(32, signed=False)) == "uint(32)"
    assert str(enc.float(16)) == "float(16)"
    assert str(enc.bool(nullable=True)) == "bool?"
