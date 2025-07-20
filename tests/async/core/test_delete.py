import pytest

from tunqi import Database, Row

pytestmark = pytest.mark.asyncio


async def test_delete_one(db: Database, r1: Row, r2: Row) -> None:
    r1_pk, r2_pk = await db.insert("t", r1, r2)
    assert await db.count("t") == 2
    assert await db.delete("t", pk=r1_pk) == 1
    assert await db.count("t") == 1
    assert await db.delete("t", pk=r2_pk) == 1
    assert await db.count("t") == 0


async def test_delete_many(db: Database, r1: Row, r2: Row) -> None:
    r1["n"] = 1
    await db.insert("t", r1, r2)
    assert await db.count("t") == 2
    assert await db.delete("t", n=1) == 2
    assert await db.count("t") == 0
    assert await db.delete("t", n=1) == 0
