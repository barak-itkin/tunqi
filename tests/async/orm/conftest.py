from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator

import pytest
import pytest_asyncio
from pydantic import BaseModel

from tunqi import Database, Index, Model, Row


class T(Model):
    b: bool
    n: int
    x: float
    s: str
    o: str | None
    dt: datetime | None
    bs: bytes
    d: dict[str, Any]
    ns: Index[list[int]]
    ss: list[str]
    f: F | None
    fs: list[F]


class F(BaseModel):
    s: str | None


@pytest_asyncio.fixture(autouse=True)
async def db(db: Database, db_url: str) -> AsyncIterator[Database]:
    model_classes = Model.config.classes.copy()
    await Model.create_tables()
    yield db
    Model.config.classes = model_classes


@pytest.fixture
def t1(r1: Row) -> T:
    return T(**r1)


@pytest.fixture
def t2(r2: Row) -> T:
    return T(**r2)


@pytest.fixture
def ts(rs: list[Row]) -> list[T]:
    return [T(**r) for r in rs]
