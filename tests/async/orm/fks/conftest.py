from __future__ import annotations

from typing import AsyncIterator

import pytest_asyncio

from tunqi import FK, M2M, Backref, Database, Model


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


@pytest_asyncio.fixture
async def create_model(db: Database) -> AsyncIterator[None]:
    model_classes = Model._config.classes.copy()
    await Model.create_tables()
    yield
    Model._config.classes = model_classes


@pytest_asyncio.fixture
async def user1(create_model: None) -> User:
    user1 = User(name="user 1")
    await user1.save()
    return user1


@pytest_asyncio.fixture
async def user2(create_model: None) -> User:
    user2 = User(name="user 2")
    await user2.save()
    return user2


@pytest_asyncio.fixture
async def post1a(user1: User) -> Post:
    post1a = Post(user=user1, content="post 1a")
    await post1a.save()
    return post1a


@pytest_asyncio.fixture
async def post1b(user1: User) -> Post:
    post1b = Post(user=user1, content="post 1b")
    await post1b.save()
    return post1b


@pytest_asyncio.fixture
async def post2a(user2: User) -> Post:
    post2a = Post(user=user2, content="post 2a")
    await post2a.save()
    return post2a


@pytest_asyncio.fixture
async def post2b(user2: User) -> Post:
    post2b = Post(user=user2, content="post 2b")
    await post2b.save()
    return post2b


@pytest_asyncio.fixture
async def comment1aX(post1a: Post) -> Comment:
    comment1aX = Comment(post=post1a, content="comment 1aX")
    await comment1aX.save()
    return comment1aX


@pytest_asyncio.fixture
async def comment1aY(post1a: Post) -> Comment:
    comment1aY = Comment(post=post1a, content="comment 1aY")
    await comment1aY.save()
    return comment1aY


@pytest_asyncio.fixture
async def comment1bX(post1b: Post) -> Comment:
    comment1bX = Comment(post=post1b, content="comment 1bX")
    await comment1bX.save()
    return comment1bX


@pytest_asyncio.fixture
async def comment2aX(post2a: Post) -> Comment:
    comment2aX = Comment(post=post2a, content="comment 2aX")
    await comment2aX.save()
    return comment2aX


@pytest_asyncio.fixture
async def tag1() -> Tag:
    tag1 = Tag(name="tag 1")
    await tag1.save()
    return tag1


@pytest_asyncio.fixture
async def tag2() -> Tag:
    tag2 = Tag(name="tag 2")
    await tag2.save()
    return tag2


@pytest_asyncio.fixture
async def tag3() -> Tag:
    tag3 = Tag(name="tag 3")
    await tag3.save()
    return tag3
