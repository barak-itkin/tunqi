from typing import Any

from tunqi.sync import Database, Row


def test_exists(db: Database, r1: Row) -> None:
    assert not db.exists("t")
    db.insert("t", r1)
    assert db.exists("t")


def test_exists_with_filter(db: Database, r2: Row, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert not db.exists("t", **{key: value})
    db.insert("t", r2)
    assert db.exists("t", **{key: value}) is expected
