from .conftest import T


def test_repr(t1: T, t2: T) -> None:
    a1 = ", ".join(f"{key}={value!r}" for key, value in t1.model_dump().items() if key != "pk")
    a2 = ", ".join(f"{key}={value!r}" for key, value in t2.model_dump().items() if key != "pk")
    s = "T({}, {})"
    assert str(t1) == s.format("?", a1)
    assert repr(t1) == s.format("?", a1)
    assert str(t2) == s.format("?", a2)
    assert repr(t2) == s.format("?", a2)
    t1.save()
    t2.save()
    assert str(t1) == s.format(t1.pk, a1)
    assert repr(t1) == s.format(t1.pk, a1)
    assert str(t2) == s.format(t2.pk, a2)
    assert repr(t2) == s.format(t2.pk, a2)


def test_eq(t1: T, t2: T) -> None:
    t1b = T(**t1.model_dump())
    t2b = T(**t2.model_dump())
    assert t1 == t1b
    assert t2 == t2b
    assert t1 != t2


def test_base():
    pass


def test_get_model():
    pass


def test_get_table():
    pass


def test_use():
    pass


def test_create_and_drop_tables():
    pass


def test_execute():
    pass


def test_transaction():
    pass


def test_refresh():
    pass


def test_deduplication():
    pass
