import enc


def test_int():
    assert str(enc.int()) == "int(_)"
    assert str(enc.int(32)) == "int(32)"
    assert str(enc.uint(32)) == "uint(32)"
    assert str(enc.float(16)) == "float(16)"
    assert str(enc.bool(nullable=True)) == "bool?"
