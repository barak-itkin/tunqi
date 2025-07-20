from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.sql.elements import ColumnElement

from tunqi import Database, Row, Selector, function, functions, q

from ..conftest import fields

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db(db: Database, db_url: str, t: dict[str, Any], rs: list[Row]) -> Database:
    db.add_table("t", t)
    await db.create_tables()
    await db.insert("t", *rs)
    return db


async def test_query_string(db: Database) -> None:
    queries = [
        (q(n=1), "n == 1"),
        (q(n__gt=4), "n > 4"),
        (~q(n__gt=4), "not n > 4"),
        (~~q(n__gt=4), "n > 4"),
        (q(n__ge=1, n__le=5), "n >= 1 and n <= 5"),
        (q("or", n__lt=1, n__gt=5), "n < 1 or n > 5"),
        (q(n__lt=1) | q(n__gt=5), "n < 1 or n > 5"),
        (~q(n__lt=1) | q(n__gt=5), "not n < 1 or n > 5"),
        (~(q(n__lt=1) | q(n__gt=5)), "not (n < 1 or n > 5)"),
        (q(n__ge=1) & ~(q(n__lt=1) | q(n__gt=5)), "n >= 1 and not (n < 1 or n > 5)"),
        (q(n__ge=1) & ~(q("or", n__lt=1, n__gt=5) & q(n__le=8)), "n >= 1 and not ((n < 1 or n > 5) and n <= 8)"),
    ]
    for query, string in queries:
        assert str(query) == string
        assert repr(query) == f"<query {string!r}>"


async def test_query(db: Database) -> None:
    assert await fields(db.select("t", "n", where=q(n=1))) == {1}
    assert await fields(db.select("t", "n", where=q(n__gt=4))) == {5, 6, 7, 8, 9}
    assert await fields(db.select("t", "n", where=q(n__le=4))) == {0, 1, 2, 3, 4}


async def test_query_with_filter(db: Database) -> None:
    assert await fields(db.select("t", "n", where=q(n__ge=1), n__le=5)) == {1, 2, 3, 4, 5}


async def test_and(db: Database) -> None:
    assert await fields(db.select("t", "n", where=q(n__ge=1, n__le=5))) == {1, 2, 3, 4, 5}
    assert await fields(db.select("t", "n", where=q("and", n__ge=1, n__le=5))) == {1, 2, 3, 4, 5}
    assert await fields(db.select("t", "n", where=q(n__ge=1) & q(n__le=5))) == {1, 2, 3, 4, 5}


async def test_or(db: Database) -> None:
    assert await fields(db.select("t", "n", where=q("or", n__lt=1, n__gt=5))) == {0, 6, 7, 8, 9}
    assert await fields(db.select("t", "n", where=q(n__lt=1) | q(n__gt=5))) == {0, 6, 7, 8, 9}


async def test_not(db: Database) -> None:
    assert await fields(db.select("t", "n", where=~q(n__gt=4))) == {0, 1, 2, 3, 4}
    assert await fields(db.select("t", "n", where=~~q(n__gt=4))) == {5, 6, 7, 8, 9}
    assert await fields(db.select("t", "n", where=~q(n__ge=1, n__le=5))) == {0, 6, 7, 8, 9}
    assert await fields(db.select("t", "n", where=~(q(n__lt=1) | q(n__gt=5)))) == {1, 2, 3, 4, 5}


async def test_compound(db: Database) -> None:
    query = q(n__gt=1) & ~(q(n__lt=1) | q(n__gt=5))
    assert await fields(db.select("t", "n", where=query, n__lt=5)) == {2, 3, 4}
    query = q(n__ge=1) & ~(q("or", n__lt=1, n__gt=5) & q(n__le=8))
    assert await fields(db.select("t", "n", where=query)) == {1, 2, 3, 4, 5, 9}


async def test_query_invalid(db: Database) -> None:
    with pytest.raises(TypeError):
        q(n__ge=1) & True
    with pytest.raises(TypeError):
        q(n__lt=1) | False


async def test_query_join(db: Database) -> None:
    db.add_table("x", {"columns": {"s": {"type": "string"}}})
    db.add_table("y", {"columns": {"x": {"type": "fk", "table": "x"}, "n": {"type": "integer"}}})
    await db.create_tables()
    pk1, pk2, pk3 = await db.insert("x", {"s": "a"}, {"s": "b"}, {"s": "c"})
    await db.insert("y", {"x": pk1, "n": 1}, {"x": pk2, "n": 2}, {"x": pk3, "n": 3}, {"x": pk3, "n": 4})
    query = ~(q(n=2) | q(x__s__gt="a"))
    assert str(query) == "not (n == 2 or x.s > 'a')"
    assert await fields(db.select("y", "n", where=query)) == {1}
    query = ~(q(s="b") | q(ys__n__gt=1))
    assert str(query) == "not (s == 'b' or ys.n > 1)"
    assert await fields(db.select("x", "s", where=query)) == {"a"}
    assert await fields(db.select("y", "n", where=q(x=pk3))) == {3, 4}
    assert await fields(db.select("x", "s", where=q(ys__gt=1))) == {"b", "c"}


async def test_custom_operator(db: Database) -> None:
    @function("->", name="next")
    def next_(selector: Selector, value: Any) -> ColumnElement:
        return selector.clause + 1 == value

    @function
    def prev(selector: Selector, value: Any) -> ColumnElement:
        return selector.clause - 1 == value

    try:
        assert str(q(n__next=5)) == "n -> 5"
        assert str(q(n__prev=5)) == "n prev 5"
        assert await fields(db.select("t", "n", where=q(n__next=5))) == {4}
        assert await fields(db.select("t", "n", where=q(n__prev=5))) == {6}
    finally:
        del functions["next"], functions["prev"]


async def test_custom_unary_operator(db: Database) -> None:
    @function("{selector}:even")
    def even(selector: Selector, value: Any) -> ColumnElement:
        return selector.clause % 2 == (0 if value else 1)

    try:
        assert str(q(n__even=True)) == "n:even"
        assert await fields(db.select("t", "n", where=q(n__even=True))) == {0, 2, 4, 6, 8}
        assert await fields(db.select("t", "n", where=q(n__even=False))) == {1, 3, 5, 7, 9}
    finally:
        del functions["even"]
