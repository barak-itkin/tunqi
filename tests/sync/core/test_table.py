import re
from typing import Any

import pytest

from tunqi.sync import Database


def test_table(db: Database) -> None:
    db.add_table("a", {"columns": {"n": {"type": "integer"}}})
    db.add_table("b", {"plural": "bi", "columns": {"s": {"type": "string"}}})
    a = db.get_table("a")
    assert str(a) == "table 'a'"
    assert repr(a) == "<table 'a'>"
    assert a.database is db
    assert a.name == "a"
    assert a.plural == "as"
    b = db.get_table("b")
    assert str(b) == "table 'b'"
    assert repr(b) == "<table 'b'>"
    assert b.database is db
    assert b.name == "b"
    assert b.plural == "bi"


def test_table_already_exists(db: Database) -> None:
    db.add_table("a", {"columns": {"n": {"type": "integer"}}})
    error = "table 'a' already exists"
    with pytest.raises(ValueError, match=re.escape(error)):
        db.add_table("a", {"columns": {"s": {"type": "string"}}})


def test_table_does_not_exist(db: Database) -> None:
    error = "table 'a' doesn't exist (available tables are t)"
    with pytest.raises(ValueError, match=re.escape(error)):
        db.get_table("a")
    with pytest.raises(ValueError, match=re.escape(error)):
        db.remove_table("a")


def test_relations(
    db: Database,
    user: dict[str, Any],
    post: dict[str, Any],
    comment: dict[str, Any],
    tag: dict[str, Any],
) -> None:
    db.add_table("user", user)
    db.add_table("post", post)
    db.add_table("comment", comment)
    db.add_table("tag", tag)
    user_table = db.get_table("user")
    post_table = db.get_table("post")
    comment_table = db.get_table("comment")
    tag_table = db.get_table("tag")
    assert list(user_table.relations) == ["posts"]
    assert list(post_table.relations) == ["user", "commentary", "tagging"]
    assert list(comment_table.relations) == ["post"]
    assert list(tag_table.relations) == ["posts"]


def test_invalid_fk(db: Database, comment: dict[str, Any]) -> None:
    db.add_table("comment", comment)
    error = "table 'post' referenced by foreign key 'comment.post' doesn't exist (available tables are t and comment)"
    with pytest.raises(ValueError, match=re.escape(error)):
        db.get_table("comment").relations


def test_invalid_m2m(db: Database, tag: dict[str, Any]) -> None:
    db.add_table("tag", tag)
    error = "table 'post' referenced by many-to-many 'tag.posts' doesn't exist (available tables are t and tag)"
    with pytest.raises(ValueError, match=re.escape(error)):
        db.get_table("tag").relations
