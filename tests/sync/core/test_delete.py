from tunqi.sync import Database, Row


def test_delete_one(db: Database, r1: Row, r2: Row) -> None:
    r1_pk, r2_pk = db.insert("t", r1, r2)
    assert db.count("t") == 2
    assert db.delete("t", pk=r1_pk) == 1
    assert db.count("t") == 1
    assert db.delete("t", pk=r2_pk) == 1
    assert db.count("t") == 0


def test_delete_many(db: Database, r1: Row, r2: Row) -> None:
    r1["n"] = 1
    db.insert("t", r1, r2)
    assert db.count("t") == 2
    assert db.delete("t", n=1) == 2
    assert db.count("t") == 0
    assert db.delete("t", n=1) == 0
