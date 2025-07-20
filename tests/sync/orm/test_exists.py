from typing import Any

from .conftest import T


def test_exists(t1: T) -> None:
    assert not T.exists()
    t1.save()
    assert T.exists()


def test_exists_pk(t1: T) -> None:
    t1.save()
    assert t1.pk
    assert T.exists(t1.pk)
    assert not T.exists(t1.pk + 1)


def test_exists_with_query(t2: T, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert not T.exists(**{key: value})
    t2.save()
    assert T.exists(**{key: value}) is expected  # noqa: PLR2004
