from typing import Any

from tunqi.sync import Database, Row


def test_count(db: Database, r1: Row, r2: Row) -> None:
    assert db.count("t") == 0
    db.insert("t", r1)
    assert db.count("t") == 1
    db.insert("t", r2)
    assert db.count("t") == 2


def test_count_with_filter(db: Database, r2: Row, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert db.count("t", **{key: value}) == 0
    db.insert("t", r2)
    assert db.count("t", **{key: value}) == (1 if expected else 0)


def test_count_distinct(db: Database, r1: Row, r2: Row) -> None:
    r1["n"] = r2["n"] = 1
    r1["s"] = "bar"
    assert r2["s"] == "foo"
    assert db.count("t", "n") == 0
    assert db.count("t", ["n", "s"]) == 0
    db.insert("t", r1)
    assert db.count("t", "n") == 1
    assert db.count("t", ["n", "s"]) == 1
    db.insert("t", r2)
    assert db.count("t", "n") == 1
    assert db.count("t", ["n", "s"]) == 2
    db.update("t", s="bar")(s="foo")
    assert db.count("t", ["n", "s"]) == 1
    assert db.count("t") == 2
