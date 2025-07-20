from typing import Any

import pytest

from tunqi import Database, Row

pytestmark = pytest.mark.asyncio


async def test_count(db: Database, r1: Row, r2: Row) -> None:
    assert await db.count("t") == 0
    await db.insert("t", r1)
    assert await db.count("t") == 1
    await db.insert("t", r2)
    assert await db.count("t") == 2


async def test_count_with_filter(db: Database, r2: Row, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert await db.count("t", **{key: value}) == 0
    await db.insert("t", r2)
    assert await db.count("t", **{key: value}) == (1 if expected else 0)


async def test_count_distinct(db: Database, r1: Row, r2: Row) -> None:
    r1["n"] = r2["n"] = 1
    r1["s"] = "bar"
    assert r2["s"] == "foo"
    assert await db.count("t", "n") == 0
    assert await db.count("t", ["n", "s"]) == 0
    await db.insert("t", r1)
    assert await db.count("t", "n") == 1
    assert await db.count("t", ["n", "s"]) == 1
    await db.insert("t", r2)
    assert await db.count("t", "n") == 1
    assert await db.count("t", ["n", "s"]) == 2
    await db.update("t", s="bar")(s="foo")
    assert await db.count("t", ["n", "s"]) == 1
    assert await db.count("t") == 2
