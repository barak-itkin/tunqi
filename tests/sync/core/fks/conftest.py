from typing import Any

import pytest

from tunqi.sync import Database, Row


@pytest.fixture
def db(
    db: Database,
    db_url: str,
    user: dict[str, Any],
    post: dict[str, Any],
    comment: dict[str, Any],
    tag: dict[str, Any],
) -> Database:
    db.add_table("user", user)
    db.add_table("post", post)
    db.add_table("comment", comment)
    db.add_table("tag", tag)
    db.create_tables()
    return db


def create(db: Database, table_name: str, row: Row) -> Row:
    [pk] = db.insert(table_name, row)
    row["pk"] = pk
    return row


@pytest.fixture
def user1(db: Database) -> Row:
    return create(db, "user", {"name": "user 1"})


@pytest.fixture
def user2(db: Database) -> Row:
    return create(db, "user", {"name": "user 2"})


@pytest.fixture
def post1a(db: Database, user1: Row) -> Row:
    return create(db, "post", {"user": user1["pk"], "content": "post 1a"})


@pytest.fixture
def post1b(db: Database, user1: Row) -> Row:
    return create(db, "post", {"user": user1["pk"], "content": "post 1b"})


@pytest.fixture
def post2a(db: Database, user2: Row) -> Row:
    return create(db, "post", {"user": user2["pk"], "content": "post 2a"})


@pytest.fixture
def post2b(db: Database, user2: Row) -> Row:
    return create(db, "post", {"user": user2["pk"], "content": "post 2b"})


@pytest.fixture
def comment1aX(db: Database, post1a: Row) -> Row:
    return create(db, "comment", {"post": post1a["pk"], "content": "comment 1aX"})


@pytest.fixture
def comment1aY(db: Database, post1a: Row) -> Row:
    return create(db, "comment", {"post": post1a["pk"], "content": "comment 1aY"})


@pytest.fixture
def comment1bX(db: Database, post1b: Row) -> Row:
    return create(db, "comment", {"post": post1b["pk"], "content": "comment 1bX"})


@pytest.fixture
def comment2aX(db: Database, post2a: Row) -> Row:
    return create(db, "comment", {"post": post2a["pk"], "content": "comment 2aX"})


@pytest.fixture
def tag1(db: Database) -> Row:
    return create(db, "tag", {"name": "tag 1"})


@pytest.fixture
def tag2(db: Database) -> Row:
    return create(db, "tag", {"name": "tag 2"})


@pytest.fixture
def tag3(db: Database) -> Row:
    return create(db, "tag", {"name": "tag 3"})
