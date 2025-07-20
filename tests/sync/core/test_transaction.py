from typing import Any

import pytest

from tunqi.sync import Database


def test_transaction(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    db.create_tables()
    with pytest.raises(ValueError):
        with db.transaction():
            db.insert("u", {"s": "foo"})
            db.insert("u", {"s": "bar"})
            assert db.count("u") == 2
            raise ValueError()
    assert db.count("u") == 0

    with db.transaction():
        db.insert("u", {"s": "foo"})
        db.insert("u", {"s": "bar"})
    assert db.count("u") == 2


def test_transaction_nested(db: Database, u: dict[str, Any]) -> None:
    db.add_table("u", u)
    db.create_tables()
    with db.transaction():
        db.insert("u", {"s": "foo"})
        with pytest.raises(ValueError):
            with db.transaction():
                db.insert("u", {"s": "bar"})
                assert db.count("u") == 2
                raise ValueError()
        assert db.count("u") == 0
    assert db.count("u") == 0

    with db.transaction():
        db.insert("u", {"s": "foo"})
        with db.transaction():
            db.insert("u", {"s": "bar"})
            assert db.count("u") == 2
        assert db.count("u") == 2
    assert db.count("u") == 2
    assert db.delete("u") == 2

    with db.transaction():
        db.insert("u", {"s": "foo"})
        with pytest.raises(ValueError):
            with db.transaction(nested=True):
                db.insert("u", {"s": "bar"})
                db.insert("u", {"s": "baz"})
                assert db.count("u") == 3
                raise ValueError()
        assert db.count("u") == 1
    assert db.count("u") == 1
    assert db.delete("u") == 1

    with db.transaction():
        db.insert("u", {"s": "foo"})
        with db.transaction(nested=True):
            db.insert("u", {"s": "bar"})
            db.insert("u", {"s": "baz"})
            assert db.count("u") == 3
        assert db.count("u") == 3
    assert db.count("u") == 3
    assert db.delete("u") == 3

    with db.transaction():
        db.insert("u", {"s": "foo"})
        with db.transaction(nested=True):
            db.insert("u", {"s": "bar"})
            with pytest.raises(ValueError):
                with db.transaction():
                    db.insert("u", {"s": "baz"})
                    assert db.count("u") == 3
                    raise ValueError()
            assert db.count("u") == 1
        assert db.count("u") == 1
    assert db.count("u") == 1
