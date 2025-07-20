from tunqi.sync import Database, Row

from ...conftest import fields


def test_select(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post2a: Row,
    comment1aX: Row,
    comment2aX: Row,
) -> None:
    # post1a -> user1
    assert db.select_one("user", posts__content="post 1a") == user1
    # post2a -> user2
    assert db.select_one("user", posts__content="post 2a") == user2
    # comment1aX -> post1a -> user1
    assert db.select_one("user", posts__commentary__content="comment 1aX") == user1
    # comment2aX -> post2a -> user2
    assert db.select_one("user", posts__commentary__content="comment 2aX") == user2
    # post1a <- comment1aX
    assert db.select_one("comment", post__content="post 1a") == comment1aX
    # post2a <- comment2aX
    assert db.select_one("comment", post__content="post 2a") == comment2aX
    # user1 <- post1a <- comment1aX
    assert db.select_one("comment", post__user__name="user 1") == comment1aX
    # user2 <- post2a <- comment2aX
    assert db.select_one("comment", post__user__name="user 2") == comment2aX


def test_select_with_query(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post1b: Row,
    post2a: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
    comment2aX: Row,
) -> None:
    # post1a -> user1
    # post1b -> user1
    # post2a -> user2
    assert db.select("user", posts__content__startswith="post") == [user1, user2]
    # post1a -> user1
    # post1b -> user1
    assert db.select("user", posts__content__startswith="post 1") == [user1]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    # comment2aX -> post2a -> user2
    assert db.select("user", posts__commentary__content__startswith="comment") == [user1, user2]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    assert db.select("user", posts__commentary__content__startswith="comment 1") == [user1]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    # comment2aX -> post2a
    assert db.select("post", commentary__content__startswith="comment") == [post1a, post1b, post2a]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    assert db.select("post", commentary__content__startswith="comment 1") == [post1a, post1b]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    # user2 <- post2a <- comment2aX
    assert db.select("comment", post__user__name__startswith="user") == [
        comment1aX,
        comment1aY,
        comment1bX,
        comment2aX,
    ]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    assert db.select("comment", post__user__name__startswith="user 1") == [
        comment1aX,
        comment1aY,
        comment1bX,
    ]


def test_exists(db: Database, user1: Row, post1a: Row, comment1aX: Row) -> None:
    # post1a -> user1
    assert db.exists("user", posts__content="post 1a")
    # comment1aX -> post1a -> user1
    assert db.exists("user", posts__commentary__content="comment 1aX")
    # Delete comment1aX.
    db.delete("comment", pk=comment1aX["pk"])
    # post1a -> user1
    assert db.exists("user", posts__content="post 1a")
    # ?????????? -> post1a -> user1
    assert not db.exists("user", posts__commentary__content="comment 1aX")
    # Delete post1a.
    db.delete("post", pk=post1a["pk"])
    # ?????? -> user1
    assert not db.exists("user", posts__content="post 1a")
    # ?????????? -> ?????? -> user1
    assert not db.exists("user", posts__commentary__content="comment 1aX")


def test_count(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    post2a: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
    comment2aX: Row,
) -> None:
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert db.count("user", posts__content__startswith="post") == 2
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    assert db.count("user", posts__content__startswith="post 1") == 1
    # comment1aX -> post1a -> user1 (1)
    # comment1aY -> post1a -> user1 (1)
    # comment1bX -> post1b -> user1 (1)
    # comment2aX -> post2a -> user2 (2)
    assert db.count("user", posts__commentary__content__startswith="comment") == 2
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    # user2 <- post2a (3)
    assert db.count("post", user__name__startswith="user") == 3
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert db.count("post", user__name__startswith="user 1") == 2
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    # user2 <- post2a <- comment2aX (4)
    assert db.count("comment", post__user__name__startswith="user") == 4
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    assert db.count("comment", post__user__name__startswith="user 1") == 3


def test_count_distinct(db: Database, user1: Row, user2: Row, post1a: Row, post1b: Row, post2a: Row) -> None:
    db.update("post")(content="post")
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert db.count("post", user__name="user 1") == 2
    # post1a.content == post1b.content == "post" (1)
    assert db.count("post", "content", user__name="user 1") == 1
    db.update("user")(name="user")
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert db.count("user", posts__content="post") == 2
    # user1.name == user2.name == "user" (1)
    assert db.count("user", "name", posts__content="post") == 1


def test_update(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post2a: Row,
    comment1aX: Row,
    comment2aX: Row,
) -> None:
    # Single update down.
    # post1a -> user1
    assert db.update("user", posts__commentary__content="comment 1aX")(name="user A") == 1
    assert fields(db.select("user", "name")) == {"user A", "user 2"}
    # Single update up.
    # user1 <- post1a <- comment1aX
    assert db.update("comment", post__user__name="user A")(content="comment A") == 1
    assert fields(db.select("comment", "content")) == {"comment A", "comment 2aX"}
    # Multiple update down.
    # comment1aX -> post1a -> user1
    # comment2aX -> post2a -> user2
    assert db.update("user", posts__commentary__content__startswith="comment")(name="user B") == 2
    assert fields(db.select("user", "name")) == {"user B"}
    # Multiple update up.
    # user1 <- post1a <- comment1aX
    # user2 <- post2a <- comment2aX
    assert db.update("comment", post__user__name__startswith="user")(content="comment B") == 2
    assert fields(db.select("comment", "content")) == {"comment B"}


def test_delete(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post1b: Row,
    post2a: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
    comment2aX: Row,
) -> None:
    # Single delete down.
    # post1b -> comment1bX
    assert db.exists("comment", pk=comment1bX["pk"])
    assert db.delete("comment", post__content="post 1b") == 1
    assert not db.exists("comment", pk=comment1bX["pk"])
    # Multiple delete down.
    # post1a -> comment1aX
    # post1a -> comment1aY
    assert db.count("comment", post__content__startswith="post 1") == 2
    assert db.delete("comment", post__content__startswith="post 1") == 2
    assert not db.exists("comment", post__content__startswith="post 1")
    # Single delete up.
    # user2 <- post2a <- comment2aX
    assert db.exists("user", pk=user2["pk"])
    assert db.delete("user", posts__commentary__content="comment 2aX") == 1
    assert not db.exists("user", pk=user2["pk"])
    assert db.delete("user", posts__commentary__content="comment 2aX") == 0


def test_delete_cascade(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert db.exists("comment", pk=comment1aX["pk"])
    assert db.exists("comment", pk=comment1aY["pk"])
    # post1a <- comment1aX
    # post1a <- comment1aY
    assert db.delete("post", pk=post1a["pk"]) == 1
    assert not db.exists("comment", pk=comment1aX["pk"])
    assert not db.exists("comment", pk=comment1aY["pk"])
    # user1 <- post1b <- comment1bX
    assert db.exists("post", pk=post1b["pk"])
    assert db.exists("comment", pk=comment1bX["pk"])
    assert db.delete("user", pk=user1["pk"]) == 1
    assert not db.exists("post", pk=post1b["pk"])
    assert not db.exists("comment", pk=comment1bX["pk"])


def test_delete_set_null(db: Database):
    db.add_table("a", {"columns": {}})
    db.add_table("b", {"columns": {"a": {"type": "fk", "table": "a", "nullable": True}}})
    db.create_tables()
    [a_pk] = db.insert("a", {})
    [b_pk] = db.insert("b", {"a": a_pk})
    # b <- a
    assert db.exists("b", a__pk=a_pk)
    assert db.delete("a", pk=a_pk) == 1
    assert not db.exists("b", a__pk=a_pk)
    # But b still exists.
    b = db.select_one("b", pk=b_pk)
    assert b["a"] is None


def test_order(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post2a: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment2aX: Row,
) -> None:
    users = [user1, user2]
    assert db.select("user", order="posts.content") == users
    assert db.select("user", order="-posts.content") == users[::-1]
    assert db.select("user", order="posts.commentary.content") == users
    assert db.select("user", order="-posts.commentary.content") == users[::-1]


def test_fields(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert fields(db.select("user", ["name", "posts.content"])) == {("user 1", "post 1a"), ("user 1", "post 1b")}
    assert fields(db.select("user", ["name", "posts.commentary.content"])) == {
        ("user 1", "comment 1aX"),
        ("user 1", "comment 1aY"),
        ("user 1", "comment 1bX"),
    }
    assert fields(db.select("user", ["name", "posts.content", "posts.commentary.content"])) == {
        ("user 1", "post 1a", "comment 1aX"),
        ("user 1", "post 1a", "comment 1aY"),
        ("user 1", "post 1b", "comment 1bX"),
    }
    assert fields(db.select("comment", ["content", "post.content"])) == {
        ("comment 1aX", "post 1a"),
        ("comment 1aY", "post 1a"),
        ("comment 1bX", "post 1b"),
    }
    assert fields(db.select("comment", ["content", "post.user.name"])) == {
        ("comment 1aX", "user 1"),
        ("comment 1aY", "user 1"),
        ("comment 1bX", "user 1"),
    }
    assert fields(db.select("comment", ["content", "post.content", "post.user.name"])) == {
        ("comment 1aX", "post 1a", "user 1"),
        ("comment 1aY", "post 1a", "user 1"),
        ("comment 1bX", "post 1b", "user 1"),
    }


def test_alias(db: Database, user1: Row, post1a: Row, comment1aX: Row, comment1aY: Row) -> None:
    result = [{"U": "user 1", "C": "comment 1aX"}, {"U": "user 1", "C": "comment 1aY"}]
    assert db.select("user", ["name:U", "posts.commentary.content:C"]) == result
    assert db.select("comment", ["content:C", "post.user.name:U"]) == result


def test_table(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert db.select("user", "posts", order="posts.pk") == [
        {"posts.pk": post1a["pk"], "posts.user": user1["pk"], "posts.content": "post 1a"},
        {"posts.pk": post1b["pk"], "posts.user": user1["pk"], "posts.content": "post 1b"},
    ]
    prefix = "posts.commentary"
    assert db.select("user", prefix, order=f"{prefix}.pk") == [
        {f"{prefix}.pk": comment1aX["pk"], f"{prefix}.post": post1a["pk"], f"{prefix}.content": "comment 1aX"},
        {f"{prefix}.pk": comment1aY["pk"], f"{prefix}.post": post1a["pk"], f"{prefix}.content": "comment 1aY"},
        {f"{prefix}.pk": comment1bX["pk"], f"{prefix}.post": post1b["pk"], f"{prefix}.content": "comment 1bX"},
    ]
