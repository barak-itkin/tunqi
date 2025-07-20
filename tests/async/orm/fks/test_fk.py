import re

import pytest

from tunqi import Database, DoesNotExistError, Error, Model, OptionalFK

from ...conftest import fields
from .conftest import Comment, Post, User

pytestmark = pytest.mark.asyncio


async def test_fk(user1: User, post1a: Post) -> None:
    assert Post.user.name == "user"
    assert Post.user.source_model is Post
    assert Post.user.model is User
    assert str(Post.user) == "foreign key Post.user -> User"
    assert repr(Post.user) == "<foreign key Post.user -> User>"
    assert post1a.user.fk is Post.user
    assert post1a.user.source is post1a
    assert str(post1a.user) == "foreign key Post[1].user -> User"
    assert repr(post1a.user) == "<foreign key Post[1].user -> User>"


async def test_fk_get(user1: User, post1a: Post) -> None:
    assert await post1a.user.get() == user1
    user1.name = "user"
    await user1.save()
    assert await post1a.user.get() == user1

    post = Post(content="post")
    assert await post.user.get() is None


async def test_fk_set(user1: User, user2: User, post1a: Post, post1b: Post) -> None:
    await post1a.user.set(user2)
    assert await post1a.user.get() == user2
    post = await Post.get(post1a.pk)
    assert await post.user.get() == user2

    error = re.escape(f"can't set {post1a.user} to {post1b!r} (expected User)")
    with pytest.raises(ValueError, match=error):
        await post1a.user.set(post1b)  # type: ignore

    await user1.delete()
    error = re.escape(f"can't set {post1a.user} to the unsaved {user1}")
    with pytest.raises(ValueError, match=error):
        await post1a.user.set(user1)

    with pytest.raises(ValueError):
        post1a.user = user2


async def test_fk_pk(user1: User, user2: User, post1a: Post) -> None:
    assert post1a.user.pk == user1.pk
    assert post1a.model_dump()["user"] == user1.pk
    post1a.user.pk = user2.pk
    assert await post1a.user.get() == user2
    post = await Post.get(post1a.pk)
    assert await post.user.get() == user1
    await post.user.set(user2)
    assert post.user.pk == user2.pk
    assert post.model_dump()["user"] == user2.pk
    post = await Post.get(post1a.pk)
    assert await post.user.get() == user2

    post = Post(content="post")
    with pytest.raises(Error):
        await post.save()


async def test_fk_init(user1: User, post1a: Post) -> None:
    post = Post(content="post", user=user1.pk)
    assert post.user.pk == user1.pk
    assert await post.user.get() == user1

    post = Post(content="post", user=user1)
    assert post.user.pk == user1.pk
    assert await post.user.get() == user1

    error = re.escape(f"can't set {Post.user} to {post1a!r} (expected PK or User)")
    with pytest.raises(ValueError, match=f".*{error}.*"):
        Post(content="post", user=post1a)

    await user1.delete()
    error = re.escape(f"can't set {Post.user} to an unsaved User ({user1})")
    with pytest.raises(ValueError, match=f".*{error}.*"):
        Post(content="post", user=user1)


async def test_optional_fk_unset(user1: User, post1a: Post) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    await Model.create_tables()
    a = A()
    await a.save()
    b = B(a=a)
    await b.save()
    assert await b.a.get() == a
    await b.a.set(None)
    assert await b.a.get() is None
    b = await B.get(b.pk)
    assert await b.a.get() is None
    assert await A.exists(a.pk)


async def test_optional_fk_pk(user1: User, post1a: Post) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    await Model.create_tables()
    a = A()
    await a.save()
    b = B()
    await b.save()
    assert await b.a.get() is None
    b.a.pk = a.pk
    await b.save()
    assert await b.a.get() == a


async def test_backref(user1: User, post1a: Post) -> None:
    assert User.posts.name == "posts"
    assert User.posts.source_model is User
    assert User.posts.model is Post
    assert User.posts.to == "user"
    assert str(User.posts) == "backreference User.posts -> Post"
    assert repr(User.posts) == "<backreference User.posts -> Post>"
    assert user1.posts.backref is User.posts
    assert user1.posts.source is user1
    assert user1.posts.model is Post
    assert str(user1.posts) == "backreference User[1].posts -> Post"
    assert repr(user1.posts) == "<backreference User[1].posts -> Post>"


async def test_backref_exists(user1: User, user2: User, post1a: Post) -> None:
    assert await user1.posts.exists()
    assert not await user2.posts.exists()


async def test_backref_count(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert await user1.posts.count() == 2
    assert await user2.posts.count() == 1
    await post2a.delete()
    assert await user2.posts.count() == 0


async def test_backref_get(user1: User, post1a: Post, post1b: Post, post2a) -> None:
    assert await user1.posts.get(content__endswith="1a") == post1a
    assert await user1.posts.get(content__endswith="1b") == post1b
    error = re.escape("post with content ending with '2a' and user == 1 doesn't exist")
    with pytest.raises(DoesNotExistError, match=error):
        await user1.posts.get(content__endswith="2a")
    assert await Post.exists(content__endswith="2a")


async def test_backref_get_fields(user1: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert await user1.posts.get_fields("content", content__endswith="1a") == {"content": "post 1a"}
    assert await user1.posts.get_fields("content", content__endswith="1b") == {"content": "post 1b"}
    error = re.escape("post with content ending with '2a' and user == 1 doesn't exist")
    with pytest.raises(DoesNotExistError, match=error):
        await user1.posts.get_fields("content", content__endswith="2a")
    assert await Post.exists(content__endswith="2a")


async def test_backref_all(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert await user1.posts.all() == [post1a, post1b]
    assert await user2.posts.all() == [post2a]
    await post2a.delete()
    assert await user2.posts.all() == []
    assert await user1.posts.all(content__endswith="1a") == [post1a]
    assert await user1.posts.all(limit=1) == [post1a]
    assert await user1.posts.all(limit=1, offset=1) == [post1b]
    assert await user1.posts.all(limit=1, offset=1, order="-content") == [post1a]


async def test_backref_all_fields(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert await user1.posts.all_fields("content") == [{"content": "post 1a"}, {"content": "post 1b"}]
    assert await user2.posts.all_fields("content") == [{"content": "post 2a"}]
    await post2a.delete()
    assert await user2.posts.all_fields("content") == []
    assert await user1.posts.all_fields("content", content__endswith="1a") == [{"content": "post 1a"}]
    assert await user1.posts.all_fields("content", limit=1) == [{"content": "post 1a"}]
    assert await user1.posts.all_fields("content", limit=1, offset=1) == [{"content": "post 1b"}]
    assert await user1.posts.all_fields("content", limit=1, offset=1, order="-content") == [{"content": "post 1a"}]


async def test_backref_create(user1: User) -> None:
    post1a = Post(content="post 1a")
    post1b = Post(content="post 1b")
    await user1.posts.create(post1a, post1b)
    assert post1a.user.pk == user1.pk
    assert post1b.user.pk == user1.pk
    assert await post1a.user.get() == user1
    assert await post1b.user.get() == user1
    assert await user1.posts.all() == [post1a, post1b]


async def test_backref_update(user1: User, post1a: Post, post1b: Post) -> None:
    assert await user1.posts.update()(content="post") == 2
    await post1a.refresh()
    assert post1a.content == "post"
    await post1b.refresh()
    assert post1b.content == "post"
    assert await user1.posts.update(post1a)(content="post 1a") == 1
    assert await user1.posts.update(post1b.pk)(content="post 1b") == 1
    await Post.refresh_all(post1a, post1b)
    assert post1a.content == "post 1a"
    assert post1b.content == "post 1b"


async def test_backref_delete(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert await user1.posts.delete(content="post 1a") == 1
    assert not await Post.exists(content="post 1a")
    assert await Post.exists(content="post 1b")
    assert await user1.posts.delete(post1b) == 1
    assert post1b.pk is None
    assert not await Post.exists(content="post 1b")
    assert await Post.exists(content="post 2a")
    assert await user2.posts.delete() == 1
    assert not await Post.exists(content="post 2a")


async def test_select(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment2aX: Comment,
) -> None:
    # post1a -> user1
    assert await User.get(posts__content="post 1a") == user1
    # post2a -> user2
    assert await User.get(posts__content="post 2a") == user2
    # comment1aX -> post1a -> user1
    assert await User.get(posts__commentary__content="comment 1aX") == user1
    # comment2aX -> post2a -> user2
    assert await User.get(posts__commentary__content="comment 2aX") == user2
    # post1a <- comment1aX
    assert await Comment.get(post__content="post 1a") == comment1aX
    # post2a <- comment2aX
    assert await Comment.get(post__content="post 2a") == comment2aX
    # user1 <- post1a <- comment1aX
    assert await Comment.get(post__user__name="user 1") == comment1aX
    # user2 <- post2a <- comment2aX
    assert await Comment.get(post__user__name="user 2") == comment2aX


async def test_select_with_query(
    user1: User,
    user2: User,
    post1a: Post,
    post1b: Post,
    post2a: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
    comment2aX: Comment,
) -> None:
    # post1a -> user1
    # post1b -> user1
    # post2a -> user2
    assert await User.all(posts__content__startswith="post") == [user1, user2]
    # post1a -> user1
    # post1b -> user1
    assert await User.all(posts__content__startswith="post 1") == [user1]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    # comment2aX -> post2a -> user2
    assert await User.all(posts__commentary__content__startswith="comment") == [user1, user2]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    assert await User.all(posts__commentary__content__startswith="comment 1") == [user1]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    # comment2aX -> post2a
    assert await Post.all(commentary__content__startswith="comment") == [post1a, post1b, post2a]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    assert await Post.all(commentary__content__startswith="comment 1") == [post1a, post1b]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    # user2 <- post2a <- comment2aX
    assert await Comment.all(post__user__name__startswith="user") == [comment1aX, comment1aY, comment1bX, comment2aX]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    assert await Comment.all(post__user__name__startswith="user 1") == [comment1aX, comment1aY, comment1bX]


async def test_exists(user1: User, post1a: Post, comment1aX: Comment) -> None:
    # post1a -> user1
    assert await User.exists(posts__content="post 1a")
    # comment1aX -> post1a -> user1
    assert await User.exists(posts__commentary__content="comment 1aX")
    # Delete comment1aX.
    await comment1aX.delete()
    # post1a -> user1
    assert await User.exists(posts__content="post 1a")
    # ?????????? -> post1a -> user1
    assert not await User.exists(posts__commentary__content="comment 1aX")
    # Delete post1a.
    await post1a.delete()
    # ?????? -> user1
    assert not await User.exists(posts__content="post 1a")
    # ?????????? -> ?????? -> user1
    assert not await User.exists(posts__commentary__content="comment 1aX")


async def test_count(
    user1: User,
    post1a: Post,
    post1b: Post,
    post2a: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
    comment2aX: Comment,
) -> None:
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert await User.count(posts__content__startswith="post") == 2
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    assert await User.count(posts__content__startswith="post 1") == 1
    # comment1aX -> post1a -> user1 (1)
    # comment1aY -> post1a -> user1 (1)
    # comment1bX -> post1b -> user1 (1)
    # comment2aX -> post2a -> user2 (2)
    assert await User.count(posts__commentary__content__startswith="comment") == 2
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    # user2 <- post2a (3)
    assert await Post.count(user__name__startswith="user") == 3
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert await Post.count(user__name__startswith="user 1") == 2
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    # user2 <- post2a <- comment2aX (4)
    assert await Comment.count(post__user__name__startswith="user") == 4
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    assert await Comment.count(post__user__name__startswith="user 1") == 3


async def test_count_distinct(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    await Post.update()(content="post")
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert await Post.count(user__name="user 1") == 2
    # post1a.content == post1b.content == "post" (1)
    assert await Post.count("content", user__name="user 1") == 1
    await User.update()(name="user")
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert await User.count(posts__content="post") == 2
    # user1.name == user2.name == "user" (1)
    assert await User.count("name", posts__content="post") == 1


async def test_update(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment2aX: Comment,
) -> None:
    # Single update down.
    # post1a -> user1
    assert await User.update(posts__commentary__content="comment 1aX")(name="user A") == 1
    assert await fields(User.all_fields("name")) == {"user A", "user 2"}
    # Single update up.
    # user1 <- post1a <- comment1aX
    assert await Comment.update(post__user__name="user A")(content="comment A") == 1
    assert await fields(Comment.all_fields("content")) == {"comment A", "comment 2aX"}
    # Multiple update down.
    # comment1aX -> post1a -> user1
    # comment2aX -> post2a -> user2
    assert await User.update(posts__commentary__content__startswith="comment")(name="user B") == 2
    assert await fields(User.all_fields("name")) == {"user B"}
    # Multiple update up.
    # user1 <- post1a <- comment1aX
    # user2 <- post2a <- comment2aX
    assert await Comment.update(post__user__name__startswith="user")(content="comment B") == 2
    assert await fields(Comment.all_fields("content")) == {"comment B"}


async def test_delete(
    user1: User,
    user2: User,
    post1a: Post,
    post1b: Post,
    post2a: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
    comment2aX: Comment,
) -> None:
    # Single delete down.
    # post1b -> comment1bX
    assert await Comment.exists(comment1bX.pk)
    assert await Comment.delete_all(post__content="post 1b") == 1
    assert not await Comment.exists(comment1bX.pk)
    # Multiple delete down.
    # post1a -> comment1aX
    # post1a -> comment1aY
    assert await Comment.count(post__content__startswith="post 1") == 2
    assert await Comment.delete_all(post__content__startswith="post 1") == 2
    assert not await Comment.exists(post__content__startswith="post 1")
    # Single delete up.
    # user2 <- post2a <- comment2aX
    assert await User.exists(user2.pk)
    assert await User.delete_all(posts__commentary__content="comment 2aX") == 1
    assert not await User.exists(user2.pk)
    assert await User.delete_all(posts__commentary__content="comment 2aX") == 0


async def test_delete_cascade(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert await Comment.exists(comment1aX.pk)
    assert await Comment.exists(comment1aY.pk)
    # post1a <- comment1aX
    # post1a <- comment1aY
    await post1a.delete()
    assert not await Comment.exists(comment1aX.pk)
    assert not await Comment.exists(comment1aY.pk)
    # user1 <- post1b <- comment1bX
    assert await Post.exists(post1b.pk)
    assert await Comment.exists(comment1bX.pk)
    await user1.delete()
    assert not await Post.exists(post1b.pk)
    assert not await Comment.exists(comment1bX.pk)


async def test_delete_set_null(db: Database) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    await Model.create_tables()
    a = A()
    await a.save()
    b = B(a=a)
    await b.save()
    # b <- a
    assert await B.exists(a__pk=a.pk)
    await a.delete()
    assert not await B.exists(a__pk=a.pk)
    # But f still exists.
    b = await B.get(b.pk)
    assert b.a.pk is None


async def test_order(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment2aX: Comment,
) -> None:
    users = [user1, user2]
    assert await User.all(order="posts.content") == users
    assert await User.all(order="-posts.content") == users[::-1]
    assert await User.all(order="posts.commentary.content") == users
    assert await User.all(order="-posts.commentary.content") == users[::-1]


async def test_fields(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert await fields(User.all_fields(["name", "posts.content"])) == {("user 1", "post 1a"), ("user 1", "post 1b")}
    assert await fields(User.all_fields(["name", "posts.commentary.content"])) == {
        ("user 1", "comment 1aX"),
        ("user 1", "comment 1aY"),
        ("user 1", "comment 1bX"),
    }
    assert await fields(User.all_fields(["name", "posts.content", "posts.commentary.content"])) == {
        ("user 1", "post 1a", "comment 1aX"),
        ("user 1", "post 1a", "comment 1aY"),
        ("user 1", "post 1b", "comment 1bX"),
    }
    assert await fields(Comment.all_fields(["content", "post.content"])) == {
        ("comment 1aX", "post 1a"),
        ("comment 1aY", "post 1a"),
        ("comment 1bX", "post 1b"),
    }
    assert await fields(Comment.all_fields(["content", "post.user.name"])) == {
        ("comment 1aX", "user 1"),
        ("comment 1aY", "user 1"),
        ("comment 1bX", "user 1"),
    }
    assert await fields(Comment.all_fields(["content", "post.content", "post.user.name"])) == {
        ("comment 1aX", "post 1a", "user 1"),
        ("comment 1aY", "post 1a", "user 1"),
        ("comment 1bX", "post 1b", "user 1"),
    }


async def test_alias(user1: User, post1a: Post, comment1aX: Comment, comment1aY: Comment) -> None:
    result = [{"U": "user 1", "C": "comment 1aX"}, {"U": "user 1", "C": "comment 1aY"}]
    assert await User.all_fields(["name:U", "posts.commentary.content:C"]) == result
    assert await Comment.all_fields(["content:C", "post.user.name:U"]) == result


async def test_table(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert await User.all_fields("posts", order="posts.pk") == [
        {"posts.pk": post1a.pk, "posts.user": user1.pk, "posts.content": "post 1a"},
        {"posts.pk": post1b.pk, "posts.user": user1.pk, "posts.content": "post 1b"},
    ]
    prefix = "posts.commentary"
    assert await User.all_fields(prefix, order=f"{prefix}.pk") == [
        {f"{prefix}.pk": comment1aX.pk, f"{prefix}.post": post1a.pk, f"{prefix}.content": "comment 1aX"},
        {f"{prefix}.pk": comment1aY.pk, f"{prefix}.post": post1a.pk, f"{prefix}.content": "comment 1aY"},
        {f"{prefix}.pk": comment1bX.pk, f"{prefix}.post": post1b.pk, f"{prefix}.content": "comment 1bX"},
    ]
