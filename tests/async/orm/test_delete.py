import re

import pytest

from tunqi import Model

from .conftest import T

pytestmark = pytest.mark.asyncio


async def test_delete_one(t1: T, t2: T) -> None:
    await T.create(t1, t2)
    assert await T.count() == 2
    await t1.delete()
    assert await T.count() == 1
    await t2.delete()
    assert await T.count() == 0


async def test_delete_before_and_after() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        async def before_delete(self):
            deleted.append(self.pk)

        async def after_delete(self):
            deleted.append(self.pk)

    await Model.create_tables()
    a = A(n=1)
    await a.save()
    await a.delete()
    assert deleted == [1, None]


async def test_delete_state_restoration() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        async def after_delete(self):
            deleted.append(self.pk)
            raise ValueError()

    await Model.create_tables()
    a = A(n=1)
    await a.save()
    with pytest.raises(ValueError):
        await a.delete()
    assert a.pk == 1
    assert await A.count() == 1
    assert deleted == [None]


async def test_delete_many(t1: T, t2: T) -> None:
    t1.n = 1
    await T.create(t1, t2)
    assert await T.count() == 2
    assert await T.delete_all(n=1) == 2
    assert await T.count() == 0
    assert await T.delete_all(n=1) == 0


async def test_delete_many_before_and_after() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        async def before_delete(self):
            deleted.append(self.pk)

        async def after_delete(self):
            deleted.append(self.pk)

    await Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    await A.create(a1, a2)
    await A.delete_all(a1, a2)
    assert deleted == [1, 2, None, None]


async def delete_many_invalid_model(t1: T) -> None:
    class A(Model):
        n: int

    a = A(n=1)
    with pytest.raises(ValueError, match=re.escape(f"{a} (item #2) is a a, not a t")):
        await T.delete_all(t1, a)
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) is a t, not a a")):
        await A.delete_all(t1, a)


async def test_delete_many_nonexisting_model(t1: T, t2: T) -> None:
    await t2.save()
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) doesn't exist")):
        await T.delete_all(t1, t2)
    await t2.delete()
    await t1.save()
    with pytest.raises(ValueError, match=re.escape(f"{t2} (item #2) doesn't exist")):
        await T.delete_all(t1, t2)


async def test_delete_many_transaction() -> None:
    class A(Model):
        n: int

        async def after_delete(self):
            if self.n > 1:
                raise ValueError("n > 1")

    await Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    await A.create(a1, a2)
    with pytest.raises(ValueError, match="n > 1"):
        await A.delete_all(a1, a2)
    assert a1.pk is not None
    assert a2.pk is not None
    assert await A.count() == 2
