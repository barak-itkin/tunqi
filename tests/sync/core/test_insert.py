from typing import Any

import pytest

from tunqi.sync import AlreadyExistsError, Database, Row


def test_insert_one(db: Database, r1: Row, r2: Row) -> None:
    [r1_pk] = db.insert("t", r1)
    assert r1_pk == 1
    [r2_pk] = db.insert("t", r2)
    assert r2_pk == 2


def test_insert_many(db: Database, r1: Row, r2: Row) -> None:
    r1_pk, r2_pk = db.insert("t", r1, r2)
    assert r1_pk == 1
    assert r2_pk == 2


def test_insert_many_without_pks(db: Database, r1: Row, r2: Row) -> None:
    assert db.insert("t", r1, r2, return_pks=False) == []
    r1a, r2b = db.select("t")
    del r1a["pk"]
    del r2b["pk"]
    assert r1a == r1
    assert r2b == r2


def test_upsert(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    db.create_tables()
    db.insert("u", {"s": "foo", "n": 1, "b": True})
    db.insert("u", {"s": "bar", "n": 2, "b": True})
    with pytest.raises(AlreadyExistsError, match="u with s 'foo' already exists"):
        db.insert("u", {"s": "foo", "n": 3, "b": False})
    db.insert("u", {"s": "foo", "n": 3, "b": False}, on_conflict="s")
    r = db.select_one("u", s="foo")
    assert r["n"] == 1
    assert r["b"] is True
    db.insert("u", {"s": "foo", "n": 3, "b": False}, on_conflict="s", update="n")
    r = db.select_one("u", s="foo")
    assert r["n"] == 3
    assert r["b"] is True
    db.insert("u", {"s": "foo", "n": 3, "b": False}, on_conflict="s", update=True)
    r = db.select_one("u", s="foo")
    assert r["n"] == 3
    assert r["b"] is False


def test_mysql_unique_string_column(db: Database) -> None:
    if not db.is_mysql:
        pytest.skip("MySQL-only test")
    with pytest.raises(
        ValueError,
        match=r"invalid column 'u.s': MySQL requires unique string columns to have length",
    ):
        db.add_table("u", {"columns": {"s": {"type": "string", "unique": True}}})


def test_unique_together(db: Database, u2: dict[str, Any]) -> None:
    db.add_table("u", u2)
    db.create_tables()
    db.insert("u", {"n1": 1, "n2": 2, "s1": "a", "s2": "b"})
    db.insert("u", {"n1": 1, "n2": 3, "s1": "b", "s2": "c"})
    with pytest.raises(AlreadyExistsError, match="u with n1 '1' and n2 '2' already exists"):
        db.insert("u", {"n1": 1, "n2": 2, "s1": "d", "s2": "e"})
    with pytest.raises(AlreadyExistsError, match="u with s1 'b' and s2 'c' already exists"):
        db.insert("u", {"n1": 3, "n2": 4, "s1": "b", "s2": "c"})
