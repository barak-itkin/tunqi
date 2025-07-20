import re
from typing import Any

import pytest

from tunqi import Database, DoesNotExistError, Row

pytestmark = pytest.mark.asyncio


async def test_select_one(db: Database, r1: Row, r2: Row) -> None:
    with pytest.raises(DoesNotExistError, match="no ts exist"):
        await db.select_one("t")
    r1_pk, r2_pk = await db.insert("t", r1, r2)
    r1["pk"], r2["pk"] = r1_pk, r2_pk
    assert await db.select_one("t", pk=r1_pk) == r1
    assert await db.select_one("t", pk=r2_pk) == r2
    error = "t with n == 2 doesn't exist"
    with pytest.raises(DoesNotExistError, match=re.escape(error)):
        await db.select_one("t", n=2)


async def test_select(db: Database, r1: Row, r2: Row) -> None:
    r1["pk"], r2["pk"] = await db.insert("t", r1, r2)
    assert await db.select("t") == [r1, r2]
    await db.delete("t")
    assert await db.select("t") == []


async def test_select_with_query(db: Database, r2: Row, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert await db.select("t", **{key: value}) == []
    [r2["pk"]] = await db.insert("t", r2)
    assert await db.select("t", **{key: value}) == ([r2] if expected else [])


async def test_select_with_range(db: Database, rs: list[Row]) -> None:
    pks = await db.insert("t", *rs)
    for r, pk in zip(rs, pks):
        r["pk"] = pk
    assert await db.select("t", limit=5) == rs[:5]
    assert await db.select("t", limit=5, offset=3) == rs[3:8]
    assert await db.select("t", offset=5) == rs[5:]


async def test_select_with_order(db: Database, rs: list[Row]) -> None:
    pks = await db.insert("t", *rs)
    for r, pk in zip(rs, pks):
        r["pk"] = pk
    assert await db.select("t", order="+n") == rs
    assert await db.select("t", order="-n") == rs[::-1]
    assert await db.select("t", order=["+b", "n"]) == rs[1::2] + rs[::2]
    assert await db.select("t", order=["-b", "n"]) == rs[::2] + rs[1::2]
    assert await db.select("t", order=["b", "-n"]) == rs[-1::-2] + rs[-2::-2]
    assert await db.select("t", order="d.x") == rs
    assert await db.select("t", order="-d.x") == rs[::-1]


async def test_select_with_fields(db: Database, r2: Row) -> None:
    assert await db.select("t") == []
    await db.insert("t", r2)
    for key, value in r2.items():
        assert await db.select("t", key) == [{key: value}]
        assert await db.select_one("t", key) == {key: value}
    columns = ["b", "n", "d.x"]
    r_dict = {"b": r2["b"], "n": r2["n"], "d.x": r2["d"]["x"]}
    assert await db.select("t", columns) == [r_dict]
    assert await db.select_one("t", columns) == r_dict


async def test_select_with_alias(db: Database, r2: Row) -> None:
    await db.insert("t", r2)
    columns = ["b:B", "n:N", "d.x:X"]
    r_dict = {"B": r2["b"], "N": r2["n"], "X": r2["d"]["x"]}
    assert await db.select("t", columns) == [r_dict]
    assert await db.select_one("t", columns) == r_dict


async def test_select_with_function(db: Database, r2: Row) -> None:
    [r2["pk"]] = await db.insert("t", r2)
    assert await db.select("t", s__length__gt=2) == [r2]
    assert await db.select("t", s__length__gt=5) == []
    assert await db.select("t", s__length__double=3.0) == [r2]
    assert await db.select("t", s__binary=b"foo") == [r2]
    assert await db.select("t", d__s__length__gt=2) == [r2]
    assert await db.select("t", d__s__length__gt=5) == []
    assert await db.select("t", d__s__length__double=3.0) == [r2]
    assert await db.select("t", d__s__binary=b"foo") == [r2]


async def test_select_function(db: Database, r2: Row) -> None:
    [r2["pk"]] = await db.insert("t", r2)
    assert await db.select_one("t", "s.length") == {"s.length": 3}
    assert await db.select_one("t", "s.length.double:n") == {"n": 3.0}
    assert await db.select_one("t", "s.binary") == {"s.binary": b"foo"}
    assert await db.select_one("t", "d.s.length") == {"d.s.length": 3}
    assert await db.select_one("t", "d.s.length.double:n") == {"n": 3.0}
    assert await db.select_one("t", "d.s.binary") == {"d.s.binary": b"foo"}


async def test_select_invalid(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    error = "table 'u' has no column 'x' (available selectors are pk, s, n and b)"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select_one("u", "x")
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select_one("u", x__y=1)
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select("u", order="x")
    error = "table 'u' has no column 'x' (available columns are pk, s, n and b)"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select_one("u", x=1)
    error = "column 'u.s' is not a JSON column"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select_one("u", "s.x")
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select_one("u", s__x=1)
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.select("u", order="s.x")
