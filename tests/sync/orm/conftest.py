from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator

import pytest
from pydantic import BaseModel

from tunqi.sync import Database, Index, Model, Row


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


@pytest.fixture(autouse=True)
def db(db: Database, db_url: str) -> Iterator[Database]:
    model_classes = Model.config.classes.copy()
    Model.create_tables()
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
