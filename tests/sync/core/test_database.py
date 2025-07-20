import base64
import datetime as dt

import pytest

from tunqi.sync import Database


def test_database(db: Database, db_url: str, db_name: str) -> None:
    db_url = db_url.replace("1234", "***")
    if not db.is_sqlite:
        db_url += db_name
    assert str(db) == f"database at {db_url!r}"
    assert repr(db) == f"<database at {db_url!r}>"
    assert db.url == db_url
    if "sqlite" in db_url:
        assert db.is_sqlite
        assert not db.is_postgresql
        assert not db.is_mysql
    if "postgresql" in db_url:
        assert not db.is_sqlite
        assert db.is_postgresql
        assert not db.is_mysql
    if "mysql" in db_url:
        assert not db.is_sqlite
        assert not db.is_postgresql
        assert db.is_mysql


def test_default_database(db_url: str) -> None:
    db = Database(db_url)
    with pytest.raises(RuntimeError, match="no active nor default database"):
        Database.get()
    db.set_default()
    assert Database.get() is db
    db.stop()
    with pytest.raises(RuntimeError, match="no active nor default database"):
        Database.get()

    db = Database(db_url, default=True)
    assert Database.get() is db
    db.stop()
    with pytest.raises(RuntimeError, match="no active nor default database"):
        Database.get()


def test_active_database(db_url: str) -> None:
    db1 = Database(db_url)
    db2 = Database(db_url)
    db3 = Database(db_url)
    with db1:
        assert Database.get() is db1
    with pytest.raises(RuntimeError, match="no active nor default database"):
        Database.get()

    db3.set_default()
    assert Database.get() is db3
    with db1:
        assert Database.get() is db1
        with db2:
            assert Database.get() is db2
            with db3:
                assert Database.get() is db3
            assert Database.get() is db2
        assert Database.get() is db1
    assert Database.get() is db3
    db1.stop()
    db2.stop()
    db3.stop()


def test_invalid_dialect() -> None:
    with pytest.raises(
        RuntimeError,
        match=r"dialect 'mariadb' is not supported \(available dialects are sqlite, postgresql and mysql\)",
    ):
        Database("mariadb+mariadbconnector://localhost:1234/test")


def test_serialization(db: Database) -> None:
    now = dt.datetime.now().astimezone()
    data = {
        "n": 1,
        "s": "foo",
        "dt": now,
        "ns": [1, 2, 3],
        "d": {
            "dt": now,
            "bs": b"\x01\x02",
            "ss": ["foo", "bar"],
        },
    }
    safe = {
        "n": 1,
        "s": "foo",
        "dt": now.astimezone(dt.UTC),
        "ns": [1, 2, 3],
        "d": {
            "dt": {"datetime": now.isoformat()},
            "bs": {"bytes": base64.b64encode(b"\x01\x02").decode()},
            "ss": ["foo", "bar"],
        },
    }
    assert db.serialize(data) == safe
    assert db.deserialize(safe) == data
    assert db.serialize([data]) == [safe]
