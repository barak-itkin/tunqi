from typing import Any

import pytest_asyncio

from tunqi import Database, Row


@pytest_asyncio.fixture
async def db(
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
    await db.create_tables()
    return db


async def create(db: Database, table_name: str, row: Row) -> Row:
    [pk] = await db.insert(table_name, row)
    row["pk"] = pk
    return row


@pytest_asyncio.fixture
async def user1(db: Database) -> Row:
    return await create(db, "user", {"name": "user 1"})


@pytest_asyncio.fixture
async def user2(db: Database) -> Row:
    return await create(db, "user", {"name": "user 2"})


@pytest_asyncio.fixture
async def post1a(db: Database, user1: Row) -> Row:
    return await create(db, "post", {"user": user1["pk"], "content": "post 1a"})


@pytest_asyncio.fixture
async def post1b(db: Database, user1: Row) -> Row:
    return await create(db, "post", {"user": user1["pk"], "content": "post 1b"})


@pytest_asyncio.fixture
async def post2a(db: Database, user2: Row) -> Row:
    return await create(db, "post", {"user": user2["pk"], "content": "post 2a"})


@pytest_asyncio.fixture
async def post2b(db: Database, user2: Row) -> Row:
    return await create(db, "post", {"user": user2["pk"], "content": "post 2b"})


@pytest_asyncio.fixture
async def comment1aX(db: Database, post1a: Row) -> Row:
    return await create(db, "comment", {"post": post1a["pk"], "content": "comment 1aX"})


@pytest_asyncio.fixture
async def comment1aY(db: Database, post1a: Row) -> Row:
    return await create(db, "comment", {"post": post1a["pk"], "content": "comment 1aY"})


@pytest_asyncio.fixture
async def comment1bX(db: Database, post1b: Row) -> Row:
    return await create(db, "comment", {"post": post1b["pk"], "content": "comment 1bX"})


@pytest_asyncio.fixture
async def comment2aX(db: Database, post2a: Row) -> Row:
    return await create(db, "comment", {"post": post2a["pk"], "content": "comment 2aX"})


@pytest_asyncio.fixture
async def tag1(db: Database) -> Row:
    return await create(db, "tag", {"name": "tag 1"})


@pytest_asyncio.fixture
async def tag2(db: Database) -> Row:
    return await create(db, "tag", {"name": "tag 2"})


@pytest_asyncio.fixture
async def tag3(db: Database) -> Row:
    return await create(db, "tag", {"name": "tag 3"})
