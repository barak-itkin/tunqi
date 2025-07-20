import re

import pytest

from tunqi.sync import Database, DoesNotExistError, Error, Model, OptionalFK

from ...conftest import fields
from .conftest import Comment, Post, User


def test_fk(user1: User, post1a: Post) -> None:
    assert Post.user.name == "user"
    assert Post.user.source_model is Post
    assert Post.user.model is User
    assert str(Post.user) == "foreign key Post.user -> User"
    assert repr(Post.user) == "<foreign key Post.user -> User>"
    assert post1a.user.fk is Post.user
    assert post1a.user.source is post1a
    assert str(post1a.user) == "foreign key Post[1].user -> User"
    assert repr(post1a.user) == "<foreign key Post[1].user -> User>"


def test_fk_get(user1: User, post1a: Post) -> None:
    assert post1a.user.get() == user1
    user1.name = "user"
    user1.save()
    assert post1a.user.get() == user1

    post = Post(content="post")
    assert post.user.get() is None


def test_fk_set(user1: User, user2: User, post1a: Post, post1b: Post) -> None:
    post1a.user.set(user2)
    assert post1a.user.get() == user2
    post = Post.get(post1a.pk)
    assert post.user.get() == user2

    error = re.escape(f"can't set {post1a.user} to {post1b!r} (expected User)")
    with pytest.raises(ValueError, match=error):
        post1a.user.set(post1b)  # type: ignore

    user1.delete()
    error = re.escape(f"can't set {post1a.user} to the unsaved {user1}")
    with pytest.raises(ValueError, match=error):
        post1a.user.set(user1)

    with pytest.raises(ValueError):
        post1a.user = user2


def test_fk_pk(user1: User, user2: User, post1a: Post) -> None:
    assert post1a.user.pk == user1.pk
    assert post1a.model_dump()["user"] == user1.pk
    post1a.user.pk = user2.pk
    assert post1a.user.get() == user2
    post = Post.get(post1a.pk)
    assert post.user.get() == user1
    post.user.set(user2)
    assert post.user.pk == user2.pk
    assert post.model_dump()["user"] == user2.pk
    post = Post.get(post1a.pk)
    assert post.user.get() == user2

    post = Post(content="post")
    with pytest.raises(Error):
        post.save()


def test_fk_init(user1: User, post1a: Post) -> None:
    post = Post(content="post", user=user1.pk)
    assert post.user.pk == user1.pk
    assert post.user.get() == user1

    post = Post(content="post", user=user1)
    assert post.user.pk == user1.pk
    assert post.user.get() == user1

    error = re.escape(f"can't set {Post.user} to {post1a!r} (expected PK or User)")
    with pytest.raises(ValueError, match=f".*{error}.*"):
        Post(content="post", user=post1a)

    user1.delete()
    error = re.escape(f"can't set {Post.user} to an unsaved User ({user1})")
    with pytest.raises(ValueError, match=f".*{error}.*"):
        Post(content="post", user=user1)


def test_optional_fk_unset(user1: User, post1a: Post) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    Model.create_tables()
    a = A()
    a.save()
    b = B(a=a)
    b.save()
    assert b.a.get() == a
    b.a.set(None)
    assert b.a.get() is None
    b = B.get(b.pk)
    assert b.a.get() is None
    assert A.exists(a.pk)


def test_optional_fk_pk(user1: User, post1a: Post) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    Model.create_tables()
    a = A()
    a.save()
    b = B()
    b.save()
    assert b.a.get() is None
    b.a.pk = a.pk
    b.save()
    assert b.a.get() == a


def test_backref(user1: User, post1a: Post) -> None:
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


def test_backref_exists(user1: User, user2: User, post1a: Post) -> None:
    assert user1.posts.exists()
    assert not user2.posts.exists()


def test_backref_count(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert user1.posts.count() == 2
    assert user2.posts.count() == 1
    post2a.delete()
    assert user2.posts.count() == 0


def test_backref_get(user1: User, post1a: Post, post1b: Post, post2a) -> None:
    assert user1.posts.get(content__endswith="1a") == post1a
    assert user1.posts.get(content__endswith="1b") == post1b
    error = re.escape("post with content ending with '2a' and user == 1 doesn't exist")
    with pytest.raises(DoesNotExistError, match=error):
        user1.posts.get(content__endswith="2a")
    assert Post.exists(content__endswith="2a")


def test_backref_get_fields(user1: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert user1.posts.get_fields("content", content__endswith="1a") == {"content": "post 1a"}
    assert user1.posts.get_fields("content", content__endswith="1b") == {"content": "post 1b"}
    error = re.escape("post with content ending with '2a' and user == 1 doesn't exist")
    with pytest.raises(DoesNotExistError, match=error):
        user1.posts.get_fields("content", content__endswith="2a")
    assert Post.exists(content__endswith="2a")


def test_backref_all(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert user1.posts.all() == [post1a, post1b]
    assert user2.posts.all() == [post2a]
    post2a.delete()
    assert user2.posts.all() == []
    assert user1.posts.all(content__endswith="1a") == [post1a]
    assert user1.posts.all(limit=1) == [post1a]
    assert user1.posts.all(limit=1, offset=1) == [post1b]
    assert user1.posts.all(limit=1, offset=1, order="-content") == [post1a]


def test_backref_all_fields(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert user1.posts.all_fields("content") == [{"content": "post 1a"}, {"content": "post 1b"}]
    assert user2.posts.all_fields("content") == [{"content": "post 2a"}]
    post2a.delete()
    assert user2.posts.all_fields("content") == []
    assert user1.posts.all_fields("content", content__endswith="1a") == [{"content": "post 1a"}]
    assert user1.posts.all_fields("content", limit=1) == [{"content": "post 1a"}]
    assert user1.posts.all_fields("content", limit=1, offset=1) == [{"content": "post 1b"}]
    assert user1.posts.all_fields("content", limit=1, offset=1, order="-content") == [{"content": "post 1a"}]


def test_backref_create(user1: User) -> None:
    post1a = Post(content="post 1a")
    post1b = Post(content="post 1b")
    user1.posts.create(post1a, post1b)
    assert post1a.user.pk == user1.pk
    assert post1b.user.pk == user1.pk
    assert post1a.user.get() == user1
    assert post1b.user.get() == user1
    assert user1.posts.all() == [post1a, post1b]


def test_backref_update(user1: User, post1a: Post, post1b: Post) -> None:
    assert user1.posts.update()(content="post") == 2
    post1a.refresh()
    assert post1a.content == "post"
    post1b.refresh()
    assert post1b.content == "post"
    assert user1.posts.update(post1a)(content="post 1a") == 1
    assert user1.posts.update(post1b.pk)(content="post 1b") == 1
    Post.refresh_all(post1a, post1b)
    assert post1a.content == "post 1a"
    assert post1b.content == "post 1b"


def test_backref_delete(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    assert user1.posts.delete(content="post 1a") == 1
    assert not Post.exists(content="post 1a")
    assert Post.exists(content="post 1b")
    assert user1.posts.delete(post1b) == 1
    assert post1b.pk is None
    assert not Post.exists(content="post 1b")
    assert Post.exists(content="post 2a")
    assert user2.posts.delete() == 1
    assert not Post.exists(content="post 2a")


def test_select(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment2aX: Comment,
) -> None:
    # post1a -> user1
    assert User.get(posts__content="post 1a") == user1
    # post2a -> user2
    assert User.get(posts__content="post 2a") == user2
    # comment1aX -> post1a -> user1
    assert User.get(posts__commentary__content="comment 1aX") == user1
    # comment2aX -> post2a -> user2
    assert User.get(posts__commentary__content="comment 2aX") == user2
    # post1a <- comment1aX
    assert Comment.get(post__content="post 1a") == comment1aX
    # post2a <- comment2aX
    assert Comment.get(post__content="post 2a") == comment2aX
    # user1 <- post1a <- comment1aX
    assert Comment.get(post__user__name="user 1") == comment1aX
    # user2 <- post2a <- comment2aX
    assert Comment.get(post__user__name="user 2") == comment2aX


def test_select_with_query(
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
    assert User.all(posts__content__startswith="post") == [user1, user2]
    # post1a -> user1
    # post1b -> user1
    assert User.all(posts__content__startswith="post 1") == [user1]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    # comment2aX -> post2a -> user2
    assert User.all(posts__commentary__content__startswith="comment") == [user1, user2]
    # comment1aX -> post1a -> user1
    # comment1aY -> post1a -> user1
    # comment1bX -> post1b -> user1
    assert User.all(posts__commentary__content__startswith="comment 1") == [user1]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    # comment2aX -> post2a
    assert Post.all(commentary__content__startswith="comment") == [post1a, post1b, post2a]
    # comment1aX -> post1a
    # comment1aY -> post1a
    # comment1bX -> post1b
    assert Post.all(commentary__content__startswith="comment 1") == [post1a, post1b]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    # user2 <- post2a <- comment2aX
    assert Comment.all(post__user__name__startswith="user") == [comment1aX, comment1aY, comment1bX, comment2aX]
    # user1 <- post1a <- comment1aX
    # user1 <- post1a <- comment1aY
    # user1 <- post1b <- comment1bX
    assert Comment.all(post__user__name__startswith="user 1") == [comment1aX, comment1aY, comment1bX]


def test_exists(user1: User, post1a: Post, comment1aX: Comment) -> None:
    # post1a -> user1
    assert User.exists(posts__content="post 1a")
    # comment1aX -> post1a -> user1
    assert User.exists(posts__commentary__content="comment 1aX")
    # Delete comment1aX.
    comment1aX.delete()
    # post1a -> user1
    assert User.exists(posts__content="post 1a")
    # ?????????? -> post1a -> user1
    assert not User.exists(posts__commentary__content="comment 1aX")
    # Delete post1a.
    post1a.delete()
    # ?????? -> user1
    assert not User.exists(posts__content="post 1a")
    # ?????????? -> ?????? -> user1
    assert not User.exists(posts__commentary__content="comment 1aX")


def test_count(
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
    assert User.count(posts__content__startswith="post") == 2
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    assert User.count(posts__content__startswith="post 1") == 1
    # comment1aX -> post1a -> user1 (1)
    # comment1aY -> post1a -> user1 (1)
    # comment1bX -> post1b -> user1 (1)
    # comment2aX -> post2a -> user2 (2)
    assert User.count(posts__commentary__content__startswith="comment") == 2
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    # user2 <- post2a (3)
    assert Post.count(user__name__startswith="user") == 3
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert Post.count(user__name__startswith="user 1") == 2
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    # user2 <- post2a <- comment2aX (4)
    assert Comment.count(post__user__name__startswith="user") == 4
    # user1 <- post1a <- comment1aX (1)
    # user1 <- post1a <- comment1aY (2)
    # user1 <- post1b <- comment1bX (3)
    assert Comment.count(post__user__name__startswith="user 1") == 3


def test_count_distinct(user1: User, user2: User, post1a: Post, post1b: Post, post2a: Post) -> None:
    Post.update()(content="post")
    # user1 <- post1a (1)
    # user1 <- post1b (2)
    assert Post.count(user__name="user 1") == 2
    # post1a.content == post1b.content == "post" (1)
    assert Post.count("content", user__name="user 1") == 1
    User.update()(name="user")
    # post1a -> user1 (1)
    # post1b -> user1 (1)
    # post2a -> user2 (2)
    assert User.count(posts__content="post") == 2
    # user1.name == user2.name == "user" (1)
    assert User.count("name", posts__content="post") == 1


def test_update(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment2aX: Comment,
) -> None:
    # Single update down.
    # post1a -> user1
    assert User.update(posts__commentary__content="comment 1aX")(name="user A") == 1
    assert fields(User.all_fields("name")) == {"user A", "user 2"}
    # Single update up.
    # user1 <- post1a <- comment1aX
    assert Comment.update(post__user__name="user A")(content="comment A") == 1
    assert fields(Comment.all_fields("content")) == {"comment A", "comment 2aX"}
    # Multiple update down.
    # comment1aX -> post1a -> user1
    # comment2aX -> post2a -> user2
    assert User.update(posts__commentary__content__startswith="comment")(name="user B") == 2
    assert fields(User.all_fields("name")) == {"user B"}
    # Multiple update up.
    # user1 <- post1a <- comment1aX
    # user2 <- post2a <- comment2aX
    assert Comment.update(post__user__name__startswith="user")(content="comment B") == 2
    assert fields(Comment.all_fields("content")) == {"comment B"}


def test_delete(
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
    assert Comment.exists(comment1bX.pk)
    assert Comment.delete_all(post__content="post 1b") == 1
    assert not Comment.exists(comment1bX.pk)
    # Multiple delete down.
    # post1a -> comment1aX
    # post1a -> comment1aY
    assert Comment.count(post__content__startswith="post 1") == 2
    assert Comment.delete_all(post__content__startswith="post 1") == 2
    assert not Comment.exists(post__content__startswith="post 1")
    # Single delete up.
    # user2 <- post2a <- comment2aX
    assert User.exists(user2.pk)
    assert User.delete_all(posts__commentary__content="comment 2aX") == 1
    assert not User.exists(user2.pk)
    assert User.delete_all(posts__commentary__content="comment 2aX") == 0


def test_delete_cascade(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert Comment.exists(comment1aX.pk)
    assert Comment.exists(comment1aY.pk)
    # post1a <- comment1aX
    # post1a <- comment1aY
    post1a.delete()
    assert not Comment.exists(comment1aX.pk)
    assert not Comment.exists(comment1aY.pk)
    # user1 <- post1b <- comment1bX
    assert Post.exists(post1b.pk)
    assert Comment.exists(comment1bX.pk)
    user1.delete()
    assert not Post.exists(post1b.pk)
    assert not Comment.exists(comment1bX.pk)


def test_delete_set_null(db: Database) -> None:
    class A(Model):
        pass

    class B(Model):
        a: OptionalFK[A]

    Model.create_tables()
    a = A()
    a.save()
    b = B(a=a)
    b.save()
    # b <- a
    assert B.exists(a__pk=a.pk)
    a.delete()
    assert not B.exists(a__pk=a.pk)
    # But f still exists.
    b = B.get(b.pk)
    assert b.a.pk is None


def test_order(
    user1: User,
    user2: User,
    post1a: Post,
    post2a: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment2aX: Comment,
) -> None:
    users = [user1, user2]
    assert User.all(order="posts.content") == users
    assert User.all(order="-posts.content") == users[::-1]
    assert User.all(order="posts.commentary.content") == users
    assert User.all(order="-posts.commentary.content") == users[::-1]


def test_fields(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert fields(User.all_fields(["name", "posts.content"])) == {("user 1", "post 1a"), ("user 1", "post 1b")}
    assert fields(User.all_fields(["name", "posts.commentary.content"])) == {
        ("user 1", "comment 1aX"),
        ("user 1", "comment 1aY"),
        ("user 1", "comment 1bX"),
    }
    assert fields(User.all_fields(["name", "posts.content", "posts.commentary.content"])) == {
        ("user 1", "post 1a", "comment 1aX"),
        ("user 1", "post 1a", "comment 1aY"),
        ("user 1", "post 1b", "comment 1bX"),
    }
    assert fields(Comment.all_fields(["content", "post.content"])) == {
        ("comment 1aX", "post 1a"),
        ("comment 1aY", "post 1a"),
        ("comment 1bX", "post 1b"),
    }
    assert fields(Comment.all_fields(["content", "post.user.name"])) == {
        ("comment 1aX", "user 1"),
        ("comment 1aY", "user 1"),
        ("comment 1bX", "user 1"),
    }
    assert fields(Comment.all_fields(["content", "post.content", "post.user.name"])) == {
        ("comment 1aX", "post 1a", "user 1"),
        ("comment 1aY", "post 1a", "user 1"),
        ("comment 1bX", "post 1b", "user 1"),
    }


def test_alias(user1: User, post1a: Post, comment1aX: Comment, comment1aY: Comment) -> None:
    result = [{"U": "user 1", "C": "comment 1aX"}, {"U": "user 1", "C": "comment 1aY"}]
    assert User.all_fields(["name:U", "posts.commentary.content:C"]) == result
    assert Comment.all_fields(["content:C", "post.user.name:U"]) == result


def test_table(
    user1: User,
    post1a: Post,
    post1b: Post,
    comment1aX: Comment,
    comment1aY: Comment,
    comment1bX: Comment,
) -> None:
    assert User.all_fields("posts", order="posts.pk") == [
        {"posts.pk": post1a.pk, "posts.user": user1.pk, "posts.content": "post 1a"},
        {"posts.pk": post1b.pk, "posts.user": user1.pk, "posts.content": "post 1b"},
    ]
    prefix = "posts.commentary"
    assert User.all_fields(prefix, order=f"{prefix}.pk") == [
        {f"{prefix}.pk": comment1aX.pk, f"{prefix}.post": post1a.pk, f"{prefix}.content": "comment 1aX"},
        {f"{prefix}.pk": comment1aY.pk, f"{prefix}.post": post1a.pk, f"{prefix}.content": "comment 1aY"},
        {f"{prefix}.pk": comment1bX.pk, f"{prefix}.post": post1b.pk, f"{prefix}.content": "comment 1bX"},
    ]
