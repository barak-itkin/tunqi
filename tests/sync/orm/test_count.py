from typing import Any

from .conftest import T


def test_count(t1: T, t2: T) -> None:
    assert T.count() == 0
    t1.save()
    assert T.count() == 1
    t2.save()
    assert T.count() == 2


def test_count_with_query(t2: T, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert T.count(**{key: value}) == 0
    t2.save()
    assert T.count(**{key: value}) == (1 if expected else 0)  # noqa: PLR2004


def test_count_distinct(t1: T, t2: T) -> None:
    t1.n = t2.n = 1
    t1.s = "bar"
    assert t2.s == "foo"
    assert T.count("n") == 0
    assert T.count(["n", "s"]) == 0
    t1.save()
    assert T.count("n") == 1
    assert T.count(["n", "s"]) == 1
    t2.save()
    assert T.count("n") == 1
    assert T.count(["n", "s"]) == 2
    t1.s = "foo"
    t1.save()
    assert T.count(["n", "s"]) == 1
    assert T.count() == 2
