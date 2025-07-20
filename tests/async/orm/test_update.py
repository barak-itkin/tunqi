import re
from typing import Any

import pytest

from tunqi import Model

from .conftest import T

pytestmark = pytest.mark.asyncio


async def test_update_one(t1: T) -> None:
    assert not t1.b
    assert t1.n == 0
    assert t1.s == ""
    await t1.save()
    t1.b = True
    await t1.save()
    t1 = await T.get(t1.pk)
    assert t1.b
    t1.n = 1
    t1.s = "foo"
    await t1.save()
    t1 = await T.get(t1.pk)
    assert t1.n == 1
    assert t1.s == "foo"


async def test_update_before_and_after() -> None:
    updated: list[dict[str, Any]] = []

    class A(Model):
        n: int
        s: str

        async def before_update(self):
            updated.append(self.changed())

        async def after_update(self):
            updated.append(self.changed())

    await Model.create_tables()
    a = A(n=1, s="foo")
    await a.save()
    assert updated == []
    a.n = 2
    await a.save()
    assert updated == [{"n": (1, 2)}, {}]
    a.s = "bar"
    await a.save()
    assert updated == [{"n": (1, 2)}, {}, {"s": ("foo", "bar")}, {}]


async def test_update_state_restoration() -> None:
    updated: list[dict[str, Any]] = []

    class A(Model):
        n: int

        async def after_update(self):
            updated.append(self.changed())
            raise ValueError()

    await Model.create_tables()
    a = A(n=1)
    await a.save()
    a.n = 2
    assert a.changed() == {"n": (1, 2)}
    with pytest.raises(ValueError):
        await a.save()
    assert a.n == 2
    assert updated == [{}]


async def test_update_many(t1: T, t2: T) -> None:
    t1.b = t2.b = False
    t1.n = t2.n = 1
    await T.create(t1, t2)
    assert await T.update(n=1)(b=True) == 2
    t1, t2 = await T.all()
    assert t1.b
    assert t2.b
    assert await T.update()(b=False) == 2
    t1, t2 = await T.all()
    assert not t1.b
    assert not t2.b
    assert await T.update(b=True)(n=2) == 0


async def delete_many_invalid_model(t1: T) -> None:
    class A(Model):
        n: int

    a = A(n=1)
    await a.save()
    with pytest.raises(ValueError, match=re.escape(f"{a} (item #2) is a a, not a t")):
        await T.update(t1, a)(n=2)
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) is a t, not a a")):
        await A.update(t1, a)(n=2)


async def test_update_many_nonexisting_model(t1: T, t2: T) -> None:
    await t2.save()
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) doesn't exist")):
        await T.update(t1, t2)(n=2)
    await t2.delete()
    await t1.save()
    with pytest.raises(ValueError, match=re.escape(f"{t2} (item #2) doesn't exist")):
        await T.update(t1, t2)(n=2)


async def test_update_many_specific(t1: T, t2: T) -> None:
    t1.b = t2.b = False
    t1.n = t2.n = 1
    await T.create(t1, t2)
    assert await T.update(t1, t2)(b=True) == 2
    assert t1.b
    assert t2.b
    assert await T.update(t1.pk, t2.pk)(n=2) == 2
    assert t1.n == 1
    assert t2.n == 1
    await t1.refresh()
    await t2.refresh()
    assert t1.n == 2
    assert t2.n == 2


async def test_update_many_before_and_after() -> None:
    updated: list[dict[str, Any]] = []

    class A(Model):
        n: int

        async def before_update(self):
            updated.append(self.changed())

        async def after_update(self):
            updated.append(self.changed())

    await Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    await A.create(a1, a2)
    assert await A.update(a1, a2)(n=3) == 2
    assert a1.n == 3
    assert a2.n == 3
    assert updated == [{}, {}, {}, {}]


async def test_update_many_transaction() -> None:
    class A(Model):
        n: int
        s: str

        async def after_update(self):
            if self.n > 1:
                raise ValueError("n > 1")

    await Model.create_tables()
    a1 = A(n=1, s="foo")
    a2 = A(n=2, s="bar")
    await A.create(a1, a2)
    with pytest.raises(ValueError, match="n > 1"):
        await A.update(a1, a2)(s="baz")
    assert a1.s == "foo"
    assert a2.s == "bar"
    await a1.refresh()
    await a2.refresh()
    assert a1.s == "foo"
    assert a2.s == "bar"
