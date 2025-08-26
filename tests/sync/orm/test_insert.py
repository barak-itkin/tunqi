import re
from typing import Annotated

import pytest

from tunqi.sync import AlreadyExistsError, Model, length, unique

from .conftest import T


def test_insert_one(t1: T, t2: T) -> None:
    assert t1.pk is None
    t1.save()
    assert t1.pk == 1
    assert t2.pk is None
    t2.save()
    assert t2.pk == 2


def test_insert_before_and_after() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        def before_create(self) -> None:
            created.append(self.pk)

        def after_create(self) -> None:
            created.append(self.pk)

    Model.create_tables()
    a = A(n=1)
    a.save()
    assert created == [None, 1]


def test_insert_many(t1: T, t2: T) -> None:
    assert t1.pk is None
    assert t2.pk is None
    T.create(t1, t2)
    assert t1.pk == 1
    assert t2.pk == 2


def test_insert_many_before_and_after() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        def before_create(self) -> None:
            created.append(self.pk)

        def after_create(self) -> None:
            created.append(self.pk)

    Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    A.create(a1, a2)
    assert created == [None, None, 1, 2]


def test_insert_state_restoration() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        def after_create(self):
            created.append(self.pk)
            raise ValueError()

    Model.create_tables()
    a = A(n=1)
    with pytest.raises(ValueError):
        a.save()
    assert a.pk is None
    assert A.count() == 0
    assert created == [1]


def test_insert_many_invalid_model(t1: T) -> None:
    class A(Model):
        n: int

    a = A(n=1)
    with pytest.raises(ValueError, match=re.escape(f"{a} (item #2) is a a, not a t")):
        T.create(t1, a)
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) is a t, not a a")):
        A.create(t1, a)


def test_insert_many_existing_model(t1: T, t2: T) -> None:
    t2.save()
    with pytest.raises(ValueError, match=re.escape(f"{t2} (item #2) already exists")):
        T.create(t1, t2)
    t1.save()
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) already exists")):
        T.create(t1, t2)


def test_insert_many_transaction() -> None:
    class A(Model):
        n: int

        def after_create(self):
            if self.n > 1:
                raise ValueError("n > 1")

    Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    with pytest.raises(ValueError, match="n > 1"):
        A.create(a1, a2)
    assert a1.pk is None
    assert a2.pk is None
    assert A.count() == 0


"""
def test_upsert() -> None:
    class U(Model):
        s: Unique[Annotated[str, length(255)]]
        n: int
        b: bool

    Model.create_tables()
    u1 = U(s="foo", n=1, b=True)
    u2 = U(s="bar", n=2, b=True)
    u3 = U(s="foo", n=3, b=False)
    U.create(u1, u2)
    with pytest.raises(AlreadyExistsError, match="u with s 'foo' already exists"):
        u3.save()
    U.create(u3, on_conflict="s")
    assert u3.pk == 1
    assert u3.n == 1
    assert u3.b is True
    u = U.get(s="foo")
    assert u.pk == u1.pk
    assert u.n == 1
    assert u.b is True
    u3.pk = None
    u3.n = 3
    u3.b = False
    U.create(u3, on_conflict="s", update="n")
    assert u3.pk == 1
    assert u3.n == 3
    assert u3.b is True
    u = U.get(s="foo")
    assert u.pk == u1.pk
    assert u.n == 3
    assert u.b is True
    u3.pk = None
    u3.n = 3
    u3.b = False
    U.create(u3, on_conflict="s", update=True)
    assert u3.pk == 1
    assert u3.n == 3
    assert u3.b is False
    u = U.get(s="foo")
    assert u.pk == 1
    assert u.n == 3
    assert u.b is False


def test_mysql_unique_string_column(db: Database) -> None:
    if not db.is_mysql:
        pytest.skip("MySQL-only test")
    with pytest.raises(
        ValueError,
        match=r"invalid column 'u.s': MySQL requires unique string columns to have length",
    ):
        db.add_table("u", {"columns": {"s": {"type": "string", "unique": True}}})
"""


def test_unique_together() -> None:
    class U(Model):
        n1: int
        n2: int
        unique("n1", "n2")
        s1: Annotated[str, length(255)]
        s2: Annotated[str, length(255)]
        unique("s1", "s2")

    Model.create_tables()

    u1 = U(n1=1, n2=2, s1="a", s2="b")
    u2 = U(n1=1, n2=3, s1="b", s2="c")
    u3 = U(n1=1, n2=2, s1="d", s2="e")
    u4 = U(n1=3, n2=4, s1="b", s2="c")
    U.create(u1, u2)
    with pytest.raises(AlreadyExistsError, match="u with n1 '1' and n2 '2' already exists"):
        u3.save()
    with pytest.raises(AlreadyExistsError, match="u with s1 'b' and s2 'c' already exists"):
        u4.save()
