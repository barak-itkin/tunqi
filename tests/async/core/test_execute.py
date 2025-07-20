import pytest

from tunqi import Database

pytestmark = pytest.mark.asyncio


async def test_execute(db: Database) -> None:
    async with db.execute("CREATE TABLE a (n INTEGER)", autocommit=True):
        pass
    try:
        async with db.execute("INSERT INTO a (n) VALUES (:n)", {"n": 1}, autocommit=True):
            pass
        async with db.execute("SELECT n FROM a") as cursor:
            assert cursor.scalar() == 1
    finally:
        async with db.execute("DROP TABLE a", autocommit=True):
            pass
    values = {"foo": "foo", "bar": "bar"}
    if db.is_mysql:
        statement = "SELECT CONCAT(:foo, :bar)"
    else:
        statement = "SELECT :foo || :bar"
    async with db.execute(statement, values) as cursor:
        assert cursor.scalar() == "foobar"
