import enc


def test_int():
    assert str(enc.int()) == "signed_int(_)"
    assert str(enc.int(32)) == "signed_int(32)"
    assert str(enc.int(32, signed=False)) == "unsigned_int(32)"
    assert str(enc.float(16)) == "float(16)"
    assert str(enc.bool(nullable=True)) == "bool?"
