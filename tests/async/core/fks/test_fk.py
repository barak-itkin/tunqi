import pytest

from tunqi import Database, Row

from ...conftest import fields

pytestmark = pytest.mark.asyncio


async def test_select(
    db: Database,
    user1: Row,
    user2: Row,
    post1a: Row,
    post2a: Row,
    comment1aX: Row,
    comment2aX: Row,
) -> None:
    # post1a -> user1
    assert await db.select_one("user", posts__content="post 1a") == user1
    # post2a -> user2
    assert await db.select_one("user", posts__content="post 2a") == user2
    # comment1aX -> post1a -> user1
    assert await db.select_one("user", posts__commentary__content="comment 1aX") == user1
    # comment2aX -> post2a -> user2
    assert await db.select_one("user", posts__commentary__content="comment 2aX") == user2
    # post1a <- comment1aX
    assert await db.select_one("comment", post__content="post 1a") == comment1aX
    # post2a <- comment2aX
    assert await db.select_one("comment", post__content="post 2a") == comment2aX
    # user1 <- post1a <- comment1aX
    assert await db.select_one("comment", post__user__name="user 1") == comment1aX
    # user2 <- post2a <- comment2aX
    assert await db.select_one("comment", post__user__name="user 2") == comment2aX


async def test_select_with_query(
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
    assert await db.select("user", posts__content__startswith="post") == [user1, user2]
    # post1a -> user1
    # post1b -> user1
    assert await db.select("user", posts__content__startswith="post 1") == [user1]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    # comment2aX -> post2a -> user2
    assert await db.select("user", posts__commentary__content__startswith="comment") == [user1, user2]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    assert await db.select("user", posts__commentary__content__startswith="comment 1") == [user1]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    # comment2aX -> post2a
    assert await db.select("post", commentary__content__startswith="comment") == [post1a, post1b, post2a]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    assert await db.select("post", commentary__content__startswith="comment 1") == [post1a, post1b]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    # user2 <- post2a <- comment2aX
    assert await db.select("comment", post__user__name__startswith="user") == [
        comment1aX,
        comment1aY,
        comment1bX,
        comment2aX,
    ]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    assert await db.select("comment", post__user__name__startswith="user 1") == [
        comment1aX,
        comment1aY,
        comment1bX,
    ]


async def test_exists(db: Database, user1: Row, post1a: Row, comment1aX: Row) -> None:
    # post1a -> user1
    assert await db.exists("user", posts__content="post 1a")
    # comment1aX -> post1a -> user1
    assert await db.exists("user", posts__commentary__content="comment 1aX")
    # Delete comment1aX.
    await db.delete("comment", pk=comment1aX["pk"])
    # post1a -> user1
    assert await db.exists("user", posts__content="post 1a")
    # ?????????? -> post1a -> user1
    assert not await db.exists("user", posts__commentary__content="comment 1aX")
    # Delete post1a.
    await db.delete("post", pk=post1a["pk"])
    # ?????? -> user1
    assert not await db.exists("user", posts__content="post 1a")
    # ?????????? -> ?????? -> user1
    assert not await db.exists("user", posts__commentary__content="comment 1aX")


async def test_count(
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
    assert await db.count("user", posts__content__startswith="post") == 2
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    assert await db.count("user", posts__content__startswith="post 1") == 1
    # comment1aX -> post1a -> user1 (1)
    # comment1aY -> post1a -> user1 (1)
    # comment1bX -> post1b -> user1 (1)
    # comment2aX -> post2a -> user2 (2)
    assert await db.count("user", posts__commentary__content__startswith="comment") == 2
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    # user2 <- post2a (3)
    assert await db.count("post", user__name__startswith="user") == 3
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert await db.count("post", user__name__startswith="user 1") == 2
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    # user2 <- post2a <- comment2aX (4)
    assert await db.count("comment", post__user__name__startswith="user") == 4
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    assert await db.count("comment", post__user__name__startswith="user 1") == 3


async def test_count_distinct(db: Database, user1: Row, user2: Row, post1a: Row, post1b: Row, post2a: Row) -> None:
    await db.update("post")(content="post")
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert await db.count("post", user__name="user 1") == 2
    # post1a.content == post1b.content == "post" (1)
    assert await db.count("post", "content", user__name="user 1") == 1
    await db.update("user")(name="user")
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert await db.count("user", posts__content="post") == 2
    # user1.name == user2.name == "user" (1)
    assert await db.count("user", "name", posts__content="post") == 1


async def test_update(
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
    assert await db.update("user", posts__commentary__content="comment 1aX")(name="user A") == 1
    assert await fields(db.select("user", "name")) == {"user A", "user 2"}
    # Single update up.
    # user1 <- post1a <- comment1aX
    assert await db.update("comment", post__user__name="user A")(content="comment A") == 1
    assert await fields(db.select("comment", "content")) == {"comment A", "comment 2aX"}
    # Multiple update down.
    # comment1aX -> post1a -> user1
    # comment2aX -> post2a -> user2
    assert await db.update("user", posts__commentary__content__startswith="comment")(name="user B") == 2
    assert await fields(db.select("user", "name")) == {"user B"}
    # Multiple update up.
    # user1 <- post1a <- comment1aX
    # user2 <- post2a <- comment2aX
    assert await db.update("comment", post__user__name__startswith="user")(content="comment B") == 2
    assert await fields(db.select("comment", "content")) == {"comment B"}


async def test_delete(
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
    assert await db.exists("comment", pk=comment1bX["pk"])
    assert await db.delete("comment", post__content="post 1b") == 1
    assert not await db.exists("comment", pk=comment1bX["pk"])
    # Multiple delete down.
    # post1a -> comment1aX
    # post1a -> comment1aY
    assert await db.count("comment", post__content__startswith="post 1") == 2
    assert await db.delete("comment", post__content__startswith="post 1") == 2
    assert not await db.exists("comment", post__content__startswith="post 1")
    # Single delete up.
    # user2 <- post2a <- comment2aX
    assert await db.exists("user", pk=user2["pk"])
    assert await db.delete("user", posts__commentary__content="comment 2aX") == 1
    assert not await db.exists("user", pk=user2["pk"])
    assert await db.delete("user", posts__commentary__content="comment 2aX") == 0


async def test_delete_cascade(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert await db.exists("comment", pk=comment1aX["pk"])
    assert await db.exists("comment", pk=comment1aY["pk"])
    # post1a <- comment1aX
    # post1a <- comment1aY
    assert await db.delete("post", pk=post1a["pk"]) == 1
    assert not await db.exists("comment", pk=comment1aX["pk"])
    assert not await db.exists("comment", pk=comment1aY["pk"])
    # user1 <- post1b <- comment1bX
    assert await db.exists("post", pk=post1b["pk"])
    assert await db.exists("comment", pk=comment1bX["pk"])
    assert await db.delete("user", pk=user1["pk"]) == 1
    assert not await db.exists("post", pk=post1b["pk"])
    assert not await db.exists("comment", pk=comment1bX["pk"])


async def test_delete_set_null(db: Database):
    db.add_table("a", {"columns": {}})
    db.add_table("b", {"columns": {"a": {"type": "fk", "table": "a", "nullable": True}}})
    await db.create_tables()
    [a_pk] = await db.insert("a", {})
    [b_pk] = await db.insert("b", {"a": a_pk})
    # b <- a
    assert await db.exists("b", a__pk=a_pk)
    assert await db.delete("a", pk=a_pk) == 1
    assert not await db.exists("b", a__pk=a_pk)
    # But b still exists.
    b = await db.select_one("b", pk=b_pk)
    assert b["a"] is None


async def test_order(
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
    assert await db.select("user", order="posts.content") == users
    assert await db.select("user", order="-posts.content") == users[::-1]
    assert await db.select("user", order="posts.commentary.content") == users
    assert await db.select("user", order="-posts.commentary.content") == users[::-1]


async def test_fields(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert await fields(db.select("user", ["name", "posts.content"])) == {("user 1", "post 1a"), ("user 1", "post 1b")}
    assert await fields(db.select("user", ["name", "posts.commentary.content"])) == {
        ("user 1", "comment 1aX"),
        ("user 1", "comment 1aY"),
        ("user 1", "comment 1bX"),
    }
    assert await fields(db.select("user", ["name", "posts.content", "posts.commentary.content"])) == {
        ("user 1", "post 1a", "comment 1aX"),
        ("user 1", "post 1a", "comment 1aY"),
        ("user 1", "post 1b", "comment 1bX"),
    }
    assert await fields(db.select("comment", ["content", "post.content"])) == {
        ("comment 1aX", "post 1a"),
        ("comment 1aY", "post 1a"),
        ("comment 1bX", "post 1b"),
    }
    assert await fields(db.select("comment", ["content", "post.user.name"])) == {
        ("comment 1aX", "user 1"),
        ("comment 1aY", "user 1"),
        ("comment 1bX", "user 1"),
    }
    assert await fields(db.select("comment", ["content", "post.content", "post.user.name"])) == {
        ("comment 1aX", "post 1a", "user 1"),
        ("comment 1aY", "post 1a", "user 1"),
        ("comment 1bX", "post 1b", "user 1"),
    }


async def test_alias(db: Database, user1: Row, post1a: Row, comment1aX: Row, comment1aY: Row) -> None:
    result = [{"U": "user 1", "C": "comment 1aX"}, {"U": "user 1", "C": "comment 1aY"}]
    assert await db.select("user", ["name:U", "posts.commentary.content:C"]) == result
    assert await db.select("comment", ["content:C", "post.user.name:U"]) == result


async def test_table(
    db: Database,
    user1: Row,
    post1a: Row,
    post1b: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
) -> None:
    assert await db.select("user", "posts", order="posts.pk") == [
        {"posts.pk": post1a["pk"], "posts.user": user1["pk"], "posts.content": "post 1a"},
        {"posts.pk": post1b["pk"], "posts.user": user1["pk"], "posts.content": "post 1b"},
    ]
    prefix = "posts.commentary"
    assert await db.select("user", prefix, order=f"{prefix}.pk") == [
        {f"{prefix}.pk": comment1aX["pk"], f"{prefix}.post": post1a["pk"], f"{prefix}.content": "comment 1aX"},
        {f"{prefix}.pk": comment1aY["pk"], f"{prefix}.post": post1a["pk"], f"{prefix}.content": "comment 1aY"},
        {f"{prefix}.pk": comment1bX["pk"], f"{prefix}.post": post1b["pk"], f"{prefix}.content": "comment 1bX"},
    ]
