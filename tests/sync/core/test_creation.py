import pytest

from tunqi.sync import Database, Error


def test_create_and_drop_tables(db: Database) -> None:
    db.add_table("a", {"columns": {"n": {"type": "integer"}}})
    db.add_table("b", {"columns": {"s": {"type": "string"}}})

    with pytest.raises(Error):
        db.insert("a", {"n": 1})
    db.create_tables("a")
    db.insert("a", {"n": 1})

    with pytest.raises(Error):
        db.insert("b", {"s": "foo"})
    db.create_tables("b")
    db.insert("b", {"s": "foo"})

    db.drop_tables()
    with pytest.raises(Error):
        db.insert("a", {"n": 1})
    with pytest.raises(Error):
        db.insert("b", {"s": "foo"})

    db.create_tables()
    assert db.count("a") == 0
    assert db.count("b") == 0

    db.insert("a", {"n": 1})
    db.drop_tables("a")
    with pytest.raises(Error):
        db.insert("a", {"n": 1})

    db.insert("b", {"s": "foo"})
    db.drop_tables("b")
    with pytest.raises(Error):
        db.insert("b", {"s": "foo"})
