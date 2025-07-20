from typing import Any

import pytest

from .conftest import T

pytestmark = pytest.mark.asyncio


async def test_exists(t1: T) -> None:
    assert not await T.exists()
    await t1.save()
    assert await T.exists()


async def test_exists_pk(t1: T) -> None:
    await t1.save()
    assert t1.pk
    assert await T.exists(t1.pk)
    assert not await T.exists(t1.pk + 1)


async def test_exists_with_query(t2: T, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert not await T.exists(**{key: value})
    await t2.save()
    assert await T.exists(**{key: value}) is expected  # noqa: PLR2004
