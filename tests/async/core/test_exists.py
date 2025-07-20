from typing import Any

import pytest

from tunqi import Database, Row

pytestmark = pytest.mark.asyncio


async def test_exists(db: Database, r1: Row) -> None:
    assert not await db.exists("t")
    await db.insert("t", r1)
    assert await db.exists("t")


async def test_exists_with_filter(db: Database, r2: Row, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert not await db.exists("t", **{key: value})
    await db.insert("t", r2)
    assert await db.exists("t", **{key: value}) is expected
