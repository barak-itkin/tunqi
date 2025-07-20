from __future__ import annotations

from typing import Iterator

import pytest

from tunqi.sync import FK, M2M, Backref, Database, Model


class User(Model):
    name: str
    posts: Backref[Post]


class Post(Model):
    content: str
    user: FK[User]
    commentary: Backref[Comment]
    tagging: M2M[Tag]


class Comment(Model):
    post: FK[Post]
    content: str


class Tag(Model):
    name: str
    posts: M2M[Post]


@pytest.fixture
def create_model(db: Database) -> Iterator[None]:
    model_classes = Model.config.classes.copy()
    Model.create_tables()
    yield
    Model.config.classes = model_classes


@pytest.fixture
def user1(create_model: None) -> User:
    user1 = User(name="user 1")
    user1.save()
    return user1


@pytest.fixture
def user2(create_model: None) -> User:
    user2 = User(name="user 2")
    user2.save()
    return user2


@pytest.fixture
def post1a(user1: User) -> Post:
    post1a = Post(user=user1, content="post 1a")
    post1a.save()
    return post1a


@pytest.fixture
def post1b(user1: User) -> Post:
    post1b = Post(user=user1, content="post 1b")
    post1b.save()
    return post1b


@pytest.fixture
def post2a(user2: User) -> Post:
    post2a = Post(user=user2, content="post 2a")
    post2a.save()
    return post2a


@pytest.fixture
def post2b(user2: User) -> Post:
    post2b = Post(user=user2, content="post 2b")
    post2b.save()
    return post2b


@pytest.fixture
def comment1aX(post1a: Post) -> Comment:
    comment1aX = Comment(post=post1a, content="comment 1aX")
    comment1aX.save()
    return comment1aX


@pytest.fixture
def comment1aY(post1a: Post) -> Comment:
    comment1aY = Comment(post=post1a, content="comment 1aY")
    comment1aY.save()
    return comment1aY


@pytest.fixture
def comment1bX(post1b: Post) -> Comment:
    comment1bX = Comment(post=post1b, content="comment 1bX")
    comment1bX.save()
    return comment1bX


@pytest.fixture
def comment2aX(post2a: Post) -> Comment:
    comment2aX = Comment(post=post2a, content="comment 2aX")
    comment2aX.save()
    return comment2aX


@pytest.fixture
def tag1() -> Tag:
    tag1 = Tag(name="tag 1")
    tag1.save()
    return tag1


@pytest.fixture
def tag2() -> Tag:
    tag2 = Tag(name="tag 2")
    tag2.save()
    return tag2


@pytest.fixture
def tag3() -> Tag:
    tag3 = Tag(name="tag 3")
    tag3.save()
    return tag3
