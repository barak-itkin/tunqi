"""
import re

import pytest

from tunq import Database, Row

from ..conftest import fields

pytestmark = pytest.mark.asyncio


async def test_select(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row) -> None:
    # Link post1a to tag1.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    # Link post2a to tag2.
    await db.link("post", "tags", [post2a["pk"]], [tag2["pk"]])
    # tag1 <-> post1a
    assert await db.select_one("post", tagging__name="tag 1") == post1a
    # tag2 <-> post2a
    assert await db.select_one("post", tagging__name="tag 2") == post2a
    # post1a <-> tag1
    assert await db.select_one("tag", posts__content="post 1a") == tag1
    # post2a <-> tag2
    assert await db.select_one("tag", posts__content="post 2a") == tag2


async def test_product(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row) -> None:
    await db.link("tag", "posts", [tag1["pk"], tag2["pk"]], [post1a["pk"], post2a["pk"]])
    # tag1 <-> post1a
    # tag1 <-> post2a
    assert await db.select("post", tagging__name="tag 1") == [post1a, post2a]
    # tag2 <-> post1a
    # tag2 <-> post2a
    assert await db.select("post", tagging__name="tag 2") == [post1a, post2a]
    # post1a <-> tag1
    # post1a <-> tag2
    assert await db.select("tag", posts__content="post 1a") == [tag1, tag2]
    # post2a <-> tag1
    # post2a <-> tag2
    assert await db.select("tag", posts__content="post 2a") == [tag1, tag2]


async def test_exists(db: Database, post1a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1, tag2, tag3.
    await db.link("post", "tagging", [post1a["pk"]], [tag1["pk"], tag2["pk"], tag3["pk"]])
    # tag1 <-> post1a
    # tag2 <-> post1a
    # tag3 <-> post1a
    assert await db.exists("post", tagging__name__startswith="tag")
    # Unlink post1a from tag1.
    await db.unlink("post", "tagging", [post1a["pk"]], [tag1["pk"]])
    # ???? <-> post1a
    # tag2 <-> post1a
    # tag3 <-> post1a
    assert await db.exists("post", tagging__name__startswith="tag")
    # Unlink post1a from tag2 and tag3.
    await db.unlink("post", "tagging", [post1a["pk"]], [tag2["pk"], tag3["pk"]])
    # ???? <-> post1a
    # ???? <-> post1a
    # ???? <-> post1a
    assert not await db.exists("post", tagging__name__startswith="tag")


async def test_count(db: Database, post1a: Row, post1b: Row, post2a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a, post1b, post2a to tag1, tag2, tag3.
    await db.link("tag", "posts", [tag1["pk"], tag2["pk"], tag3["pk"]], [post1a["pk"], post1b["pk"], post2a["pk"]])
    # post1a <-> tag1, tag2, tag3 (3)
    # post1b <-> tag1, tag2, tag3 (3)
    # post2a <-> tag1, tag2, tag3 (3)
    assert await db.count("tag", posts__content__startswith="post") == 3
    # post1a <-> tag1, tag2, tag3 (3)
    # post1b <-> tag1, tag2, tag3 (3)
    assert await db.count("tag", posts__content__startswith="post 1") == 3
    # tag1 <-> post1a, post1b, post2a (3)
    # tag2 <-> post1a, post1b, post2a (3)
    # tag3 <-> post1a, post1b, post2a (3)
    assert await db.count("post", tagging__name__startswith="tag") == 3
    # tag1 <-> post1a, post1b, post2a (3)
    assert await db.count("post", tagging__name__startswith="tag 1") == 3
    # Unlink post1a from tag1.
    await db.unlink("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    # post1a <-> tag2, tag3 (2)
    # post1b <-> tag1, tag2, tag3 (3)
    # post2a <-> tag1, tag2, tag3 (3)
    assert await db.count("tag", posts__content__startswith="post") == 3
    # post1a <-> tag2, tag3 (2)
    # post1b <-> tag1, tag2, tag3 (3)
    assert await db.count("tag", posts__content__startswith="post 1") == 3
    # tag1 <-> post1b, post2a (2)
    # tag2 <-> post1a, post1b, post2a (3)
    # tag3 <-> post1a, post1b, post2a (3)
    assert await db.count("post", tagging__name__startswith="tag") == 3
    # tag1 <-> post1b, post2a (2)
    assert await db.count("post", tagging__name__startswith="tag 1") == 2
    # Unlink post1b and post2a from tag1.
    await db.unlink("tag", "posts", [tag1["pk"]], [post1b["pk"], post2a["pk"]])
    # post1a <-> tag2, tag3 (2)
    # post1b <-> tag2, tag3 (2)
    # post2a <-> tag2, tag3 (2)
    assert await db.count("tag", posts__content__startswith="post") == 2
    # post1a <-> tag2, tag3 (2)
    # post1b <-> tag2, tag3 (2)
    assert await db.count("tag", posts__content__startswith="post 1") == 2
    # tag1 <-> ? (0)
    # tag2 <-> post1a, post1b, post2a (3)
    # tag3 <-> post1a, post1b, post2a (3)
    assert await db.count("post", tagging__name__startswith="tag") == 3
    # tag1 <-> ? (0)
    assert await db.count("post", tagging__name__startswith="tag 1") == 0
    # Unlink post1a and post1b from tag2 and tag3.
    await db.unlink("tag", "posts", [tag2["pk"], tag3["pk"]], [post1a["pk"], post1b["pk"]])
    # post1a <-> ? (0)
    # post1b <-> ? (0)
    # post2a <-> tag2, tag3 (2)
    assert await db.count("tag", posts__content__startswith="post") == 2
    # post1a <-> ? (0)
    # post1b <-> ? (0)
    assert await db.count("tag", posts__content__startswith="post 1") == 0
    # tag1 <-> ? (0)
    # tag2 <-> post2a (1)
    # tag3 <-> post2a (1)
    assert await db.count("post", tagging__name__startswith="tag") == 1
    # tag1 <-> ? (0)
    assert await db.count("post", tagging__name__startswith="tag 1") == 0


async def test_count_distinct(db: Database, post1a: Row, post1b: Row, tag1: Row, tag2: Row) -> None:
    await db.update("post")(content="post")
    # Link post1a and post1b to tag1.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"], post1b["pk"]])
    # tag1 <-> post1a, post1b (2)
    assert await db.count("post", tagging__name="tag 1") == 2
    # post1a.content == post1b.content == "post" (1)
    assert await db.count("post", "content", tagging__name="tag 1") == 1
    # Link post1a and post1b to tag2.
    await db.link("tag", "posts", [tag2["pk"]], [post1a["pk"], post1b["pk"]])
    await db.update("tag")(name="tag")
    # post1a <-> tag1, tag2 (2)
    # post1b <-> tag1, tag2 (2)
    assert await db.count("tag", posts__content="post") == 2
    # tag1.name == tag2.name == "tag" (1)
    assert await db.count("tag", "name", posts__content="post") == 1


async def test_update(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    # Link post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post2a["pk"]])
    # Single update left.
    # tag1 <-> post1a
    assert await db.update("post", tagging__name="tag 1")(content="x") == 1
    assert await fields(db.select("post", "content")) == {"x", "post 2a"}
    # Single update right.
    # post1a <-> tag1
    assert await db.update("tag", posts__content="x")(name="x") == 1
    assert await fields(db.select("tag", "name")) == {"x", "tag 2", "tag 3"}
    # Multiple update left.
    # tag2 <-> post1a
    # tag2 <-> post2a
    assert await db.update("post", tagging__name__startswith="tag")(content="x") == 1
    assert await fields(db.select("post", "content")) == {"x"}
    # Multiple update right.
    # post1a <-> tag1
    # post2a <-> tag2, tag3
    assert await db.update("tag", posts__content="x")(name="x") == 3
    assert await fields(db.select("tag", "name")) == {"x"}


async def test_delete(
    db: Database,
    post1a: Row,
    post1b: Row,
    post2a: Row,
    comment1aX: Row,
    comment1aY: Row,
    comment1bX: Row,
    comment2aX: Row,
    tag1: Row,
    tag2: Row,
    tag3: Row,
):
    # Link post1a to tag1 and post1b to tag2.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    await db.link("tag", "posts", [tag2["pk"]], [post1b["pk"]])
    # Single delete left.
    # tag2 <-> post1b -> comment1bX
    assert await db.exists("comment", pk=comment1bX["pk"])
    assert await db.delete("comment", post__tagging__name="tag 2") == 1
    assert not await db.exists("comment", pk=comment1bX["pk"])
    # Multiple delete left.
    # tag1 <-> post1a -> comment1aX
    # tag1 <-> post1a -> comment1aY
    assert await db.count("comment", post__tagging__name="tag 1") == 2
    assert await db.delete("comment", post__tagging__name="tag 1") == 2
    assert not await db.exists("comment", post__tagging__name="tag 1")
    # Single delete right.
    # post1a <-> tag1
    assert await db.exists("tag", pk=tag1["pk"])
    assert await db.delete("tag", posts__content="post 1a") == 1
    assert not await db.exists("tag", pk=tag1["pk"])
    # Link post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post2a["pk"]])
    # Multiple delete right.
    # comment2aX -> post2a <-> tag3
    # comment2aX -> post2a <-> tag2
    assert await db.count("tag", posts__commentary__content="comment 2aX") == 2
    assert await db.delete("tag", posts__commentary__content="comment 2aX") == 2
    assert not await db.exists("tag", posts__commentary__content="comment 2aX")
    assert await db.delete("tag", posts__commentary__content="comment 2aX") == 0


async def test_order(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1 and tag2 and post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag1["pk"], tag2["pk"]], [post1a["pk"]])
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post2a["pk"]])
    posts = [post1a, post2a]
    # tag1 <-> post1a comes first.
    assert await db.select("post", order="tagging.name") == posts
    # tag3 <-> post2a comes first.
    assert await db.select("post", order="-tagging.name") == posts[::-1]
    # post1a <-> tag1, tag2 comes first, so tag3 should be last.
    tag_names = [tag["name"] for tag in await db.select("tag", order="posts.content")]
    assert tag_names.index("tag 1") < tag_names.index("tag 3")
    # post2a <-> tag2, tag3 comes first, so tag1 should be last.
    tag_names = [tag["name"] for tag in await db.select("tag", order="-posts.content")]
    assert tag_names.index("tag 1") > tag_names.index("tag 3")


async def test_fields(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1 and post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post2a["pk"]])
    assert await db.select("post", ["content", "tagging.name"]) == [
        {"content": "post 1a", "tagging.name": "tag 1"},
        {"content": "post 2a", "tagging.name": "tag 2"},
        {"content": "post 2a", "tagging.name": "tag 3"},
    ]
    assert await db.select("tag", ["name", "posts.content"]) == [
        {"name": "tag 1", "posts.content": "post 1a"},
        {"name": "tag 2", "posts.content": "post 2a"},
        {"name": "tag 3", "posts.content": "post 2a"},
    ]


async def test_alias(db: Database, post1a: Row, post2a: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1 and post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post2a["pk"]])
    result = [{"P": "post 1a", "T": "tag 1"}, {"P": "post 2a", "T": "tag 2"}, {"P": "post 2a", "T": "tag 3"}]
    assert await db.select("post", ["content:P", "tagging.name:T"]) == result
    assert await db.select("tag", ["name:T", "posts.content:P"]) == result


async def test_table(db: Database, user1: Row, post1a: Row, post1b: Row, tag1: Row, tag2: Row, tag3: Row) -> None:
    # Link post1a to tag1 and post2a to tag2 and tag3.
    await db.link("tag", "posts", [tag1["pk"]], [post1a["pk"]])
    await db.link("tag", "posts", [tag2["pk"], tag3["pk"]], [post1b["pk"]])
    assert await db.select("post", "tagging") == [
        {"tagging.pk": tag1["pk"], "tagging.name": "tag 1"},
        {"tagging.pk": tag2["pk"], "tagging.name": "tag 2"},
        {"tagging.pk": tag3["pk"], "tagging.name": "tag 3"},
    ]
    assert await db.select("tag", "posts") == [
        {"posts.pk": post1a["pk"], "posts.user": user1["pk"], "posts.content": "post 1a"},
        {"posts.pk": post1b["pk"], "posts.user": user1["pk"], "posts.content": "post 1b"},
        {"posts.pk": post1b["pk"], "posts.user": user1["pk"], "posts.content": "post 1b"},
    ]


async def test_invalid_link(db: Database, user1: Row, post1a: Row, tag1: Row) -> None:
    error = "table 'user' has no many-to-many relation 'tags' (available many-to-many relations are <none>)"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.link("user", "tags", [user1["pk"]], [tag1["pk"]])


async def test_invalid_link_target(db: Database, post1a: Row) -> None:
    db.remove_table("tag")
    error = "table 'tag' doesn't exist (available tables are t, user, post and comment)"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.unlink("post", "tags", [post1a["pk"]], [1])


async def test_invalid_unlink(db: Database, user1: Row, tag1: Row) -> None:
    error = "table 'user' has no many-to-many relation 'tags' (available many-to-many relations are <none>)"
    with pytest.raises(ValueError, match=re.escape(error)):
        await db.unlink("user", "tags", [user1["pk"]], [tag1["pk"]])

"""
