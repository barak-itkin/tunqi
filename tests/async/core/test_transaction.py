from typing import Any

import pytest

from tunqi import Database

pytestmark = pytest.mark.asyncio


async def test_transaction(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    await db.create_tables()
    with pytest.raises(ValueError):
        async with db.transaction():
            await db.insert("u", {"s": "foo"})
            await db.insert("u", {"s": "bar"})
            assert await db.count("u") == 2
            raise ValueError()
    assert await db.count("u") == 0

    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        await db.insert("u", {"s": "bar"})
    assert await db.count("u") == 2


async def test_transaction_nested(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    await db.create_tables()
    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        with pytest.raises(ValueError):
            async with db.transaction():
                await db.insert("u", {"s": "bar"})
                assert await db.count("u") == 2
                raise ValueError()
        assert await db.count("u") == 0
    assert await db.count("u") == 0

    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        async with db.transaction():
            await db.insert("u", {"s": "bar"})
            assert await db.count("u") == 2
        assert await db.count("u") == 2
    assert await db.count("u") == 2
    assert await db.delete("u") == 2

    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        with pytest.raises(ValueError):
            async with db.transaction(nested=True):
                await db.insert("u", {"s": "bar"})
                await db.insert("u", {"s": "baz"})
                assert await db.count("u") == 3
                raise ValueError()
        assert await db.count("u") == 1
    assert await db.count("u") == 1
    assert await db.delete("u") == 1

    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        async with db.transaction(nested=True):
            await db.insert("u", {"s": "bar"})
            await db.insert("u", {"s": "baz"})
            assert await db.count("u") == 3
        assert await db.count("u") == 3
    assert await db.count("u") == 3
    assert await db.delete("u") == 3

    async with db.transaction():
        await db.insert("u", {"s": "foo"})
        async with db.transaction(nested=True):
            await db.insert("u", {"s": "bar"})
            with pytest.raises(ValueError):
                async with db.transaction():
                    await db.insert("u", {"s": "baz"})
                    assert await db.count("u") == 3
                    raise ValueError()
            assert await db.count("u") == 1
        assert await db.count("u") == 1
    assert await db.count("u") == 1
