from typing import Any

import pytest
import pytest_asyncio

from tunqi import Database, Row, c

from ..conftest import fields

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db(db: Database, db_url: str, t: dict[str, Any], rs: list[Row]) -> Database:
    db.add_table("t", t)
    await db.create_tables()
    await db.insert("t", *rs)
    return db


async def test_expression(db: Database) -> None:
    pass


async def test_condition(db: Database) -> None:
    assert await fields(db.select("t", "n", where=c.n == 1)) == {1}
    assert await fields(db.select("t", "n", where=c.n > 4)) == {5, 6, 7, 8, 9}
    assert await fields(db.select("t", "n", where=c.n <= 4)) == {0, 1, 2, 3, 4}


async def test_selectors(db: Database, r1: Row) -> None:
    await db.insert("t", r1 | {"n": 10, "s": "foo"})
    assert await db.select("t", c.s, n=10) == [{"s": "foo"}]
    assert await db.select("t", c.s.as_("S"), n=10) == [{"S": "foo"}]
    assert await db.select("t", c.s.length().as_("L"), n=10) == [{"L": 3}]


async def test_selectors_json(db: Database, r1: Row) -> None:
    await db.insert("t", r1 | {"n": 10, "d": {"n": 1, "s": "foo"}})
    assert await db.select("t", c.d.s, n=10) == [{"d.s": "foo"}]
    assert await db.select("t", c.d.s.as_("S"), n=10) == [{"S": "foo"}]
    assert await db.select("t", c.d.s.length().as_("L") + 2, n=10) == [{"L": 5}]
    assert await db.select("t", c["d.s"], n=10) == [{"d.s": "foo"}]
    assert await db.select("t", c["d.s:S"], n=10) == [{"S": "foo"}]
    assert await db.select("t", c["d.s.length:L"] + 2, n=10) == [{"L": 5}]


async def test_increment(db: Database, r1: Row) -> None:
    await db.insert("t", r1 | {"n": 10, "x": 1})
    assert await db.update("t", n=10)(x=c.x + 1) == 1
    assert await db.select_one("t", "x", n=10) == {"x": 2.0}
