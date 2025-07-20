from typing import Any

import pytest

from .conftest import T

pytestmark = pytest.mark.asyncio


async def test_count(t1: T, t2: T) -> None:
    assert await T.count() == 0
    await t1.save()
    assert await T.count() == 1
    await t2.save()
    assert await T.count() == 2


async def test_count_with_query(t2: T, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert await T.count(**{key: value}) == 0
    await t2.save()
    assert await T.count(**{key: value}) == (1 if expected else 0)  # noqa: PLR2004


async def test_count_distinct(t1: T, t2: T) -> None:
    t1.n = t2.n = 1
    t1.s = "bar"
    assert t2.s == "foo"
    assert await T.count("n") == 0
    assert await T.count(["n", "s"]) == 0
    await t1.save()
    assert await T.count("n") == 1
    assert await T.count(["n", "s"]) == 1
    await t2.save()
    assert await T.count("n") == 1
    assert await T.count(["n", "s"]) == 2
    t1.s = "foo"
    await t1.save()
    assert await T.count(["n", "s"]) == 1
    assert await T.count() == 2
