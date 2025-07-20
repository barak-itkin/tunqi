import re
from typing import Annotated

import pytest

from tunqi import AlreadyExistsError, Model, Unique, length, unique

from .conftest import T

pytestmark = pytest.mark.asyncio


async def test_insert_one(t1: T, t2: T) -> None:
    assert t1.pk is None
    await t1.save()
    assert t1.pk == 1
    assert t2.pk is None
    await t2.save()
    assert t2.pk == 2


async def test_insert_before_and_after() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        async def before_create(self) -> None:
            created.append(self.pk)

        async def after_create(self) -> None:
            created.append(self.pk)

    await Model.create_tables()
    a = A(n=1)
    await a.save()
    assert created == [None, 1]


async def test_insert_many(t1: T, t2: T) -> None:
    assert t1.pk is None
    assert t2.pk is None
    await T.create(t1, t2)
    assert t1.pk == 1
    assert t2.pk == 2


async def test_insert_many_before_and_after() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        async def before_create(self) -> None:
            created.append(self.pk)

        async def after_create(self) -> None:
            created.append(self.pk)

    await Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    await A.create(a1, a2)
    assert created == [None, None, 1, 2]


async def test_insert_state_restoration() -> None:
    created: list[int | None] = []

    class A(Model):
        n: int

        async def after_create(self):
            created.append(self.pk)
            raise ValueError()

    await Model.create_tables()
    a = A(n=1)
    with pytest.raises(ValueError):
        await a.save()
    assert a.pk is None
    assert await A.count() == 0
    assert created == [1]


async def test_insert_many_invalid_model(t1: T) -> None:
    class A(Model):
        n: int

    a = A(n=1)
    with pytest.raises(ValueError, match=re.escape(f"{a} (item #2) is a a, not a t")):
        await T.create(t1, a)
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) is a t, not a a")):
        await A.create(t1, a)


async def test_insert_many_existing_model(t1: T, t2: T) -> None:
    await t2.save()
    with pytest.raises(ValueError, match=re.escape(f"{t2} (item #2) already exists")):
        await T.create(t1, t2)
    await t1.save()
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) already exists")):
        await T.create(t1, t2)


async def test_insert_many_transaction() -> None:
    class A(Model):
        n: int

        async def after_create(self):
            if self.n > 1:
                raise ValueError("n > 1")

    await Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    with pytest.raises(ValueError, match="n > 1"):
        await A.create(a1, a2)
    assert a1.pk is None
    assert a2.pk is None
    assert await A.count() == 0


async def test_upsert() -> None:
    class U(Model):
        s: Unique[Annotated[str, length(255)]]
        n: int
        b: bool

    await Model.create_tables()
    u1 = U(s="foo", n=1, b=True)
    u2 = U(s="bar", n=2, b=True)
    u3 = U(s="foo", n=3, b=False)
    await U.create(u1, u2)
    with pytest.raises(AlreadyExistsError, match="u with s 'foo' already exists"):
        await u3.save()
    await U.create(u3, on_conflict="s")
    assert u3.pk is None
    u = await U.get(s="foo")
    assert u.pk == u1.pk
    assert u.n == 1
    assert u.b is True
    await U.create(u3, on_conflict="s", update="n")
    assert u3.pk == u1.pk
    u = await U.get(s="foo")
    assert u.pk == u1.pk
    assert u.n == 3
    assert u.b is True
    u3.pk = None
    await U.create(u3, on_conflict="s", update=True)
    assert u3.pk == u1.pk
    u = await U.get(s="foo")
    assert u.pk == u1.pk
    assert u.n == 3
    assert u.b is False


"""
def test_mysql_unique_string_column(db: Database) -> None:
    if not db.is_mysql:
        pytest.skip("MySQL-only test")
    with pytest.raises(
        ValueError,
        match=r"invalid column 'u.s': MySQL requires unique string columns to have length",
    ):
        db.add_table("u", {"columns": {"s": {"type": "string", "unique": True}}})
"""


async def test_unique_together() -> None:
    class U(Model):
        n1: int
        n2: int
        unique("n1", "n2")
        s1: Annotated[str, length(255)]
        s2: Annotated[str, length(255)]
        unique("s1", "s2")

    await Model.create_tables()

    u1 = U(n1=1, n2=2, s1="a", s2="b")
    u2 = U(n1=1, n2=3, s1="b", s2="c")
    u3 = U(n1=1, n2=2, s1="d", s2="e")
    u4 = U(n1=3, n2=4, s1="b", s2="c")
    await U.create(u1, u2)
    with pytest.raises(AlreadyExistsError, match="u with n1 '1' and n2 '2' already exists"):
        await u3.save()
    with pytest.raises(AlreadyExistsError, match="u with s1 'b' and s2 'c' already exists"):
        await u4.save()
