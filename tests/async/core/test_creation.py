import pytest

from tunqi import Database, Error

pytestmark = pytest.mark.asyncio


async def test_create_and_drop_tables(db: Database) -> None:
    db.add_table("a", {"columns": {"n": {"type": "integer"}}})
    db.add_table("b", {"columns": {"s": {"type": "string"}}})

    with pytest.raises(Error):
        await db.insert("a", {"n": 1})
    await db.create_tables("a")
    await db.insert("a", {"n": 1})

    with pytest.raises(Error):
        await db.insert("b", {"s": "foo"})
    await db.create_tables("b")
    await db.insert("b", {"s": "foo"})

    await db.drop_tables()
    with pytest.raises(Error):
        await db.insert("a", {"n": 1})
    with pytest.raises(Error):
        await db.insert("b", {"s": "foo"})

    await db.create_tables()
    assert await db.count("a") == 0
    assert await db.count("b") == 0

    await db.insert("a", {"n": 1})
    await db.drop_tables("a")
    with pytest.raises(Error):
        await db.insert("a", {"n": 1})

    await db.insert("b", {"s": "foo"})
    await db.drop_tables("b")
    with pytest.raises(Error):
        await db.insert("b", {"s": "foo"})
