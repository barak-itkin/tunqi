import pytest

from tunqi import Database, Row

pytestmark = pytest.mark.asyncio


async def test_update_one(db: Database, r1: Row) -> None:
    assert not r1["b"]
    assert r1["n"] == 0
    assert r1["s"] == ""
    [r1_pk] = await db.insert("t", r1)
    assert await db.update("t", pk=r1_pk)(b=True) == 1
    r1 = await db.select_one("t", pk=r1_pk)
    assert r1["b"]
    assert await db.update("t", pk=r1_pk)(n=1, s="foo") == 1
    r1 = await db.select_one("t", pk=r1_pk)
    assert r1["n"] == 1
    assert r1["s"] == "foo"


async def test_update_many(db: Database, r1: Row, r2: Row) -> None:
    r1["b"] = r2["b"] = False
    r1["n"] = r2["n"] = 1
    await db.insert("t", r1, r2)
    assert await db.update("t", n=1)(b=True) == 2
    r1, r2 = await db.select("t")
    assert r1["b"]
    assert r2["b"]
    assert await db.update("t")(b=False) == 2
    r1, r2 = await db.select("t")
    assert not r1["b"]
    assert not r2["b"]
    assert await db.update("t", b=True)(n=2) == 0
