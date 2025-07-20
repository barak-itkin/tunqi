from typing import Any, Iterable

import pytest

from tunqi.utils import and_, pluralize


def test_concat_none() -> None:
    empty: Iterable[Any]
    for empty in ([], set(), (i for i in range(1, 1))):
        assert and_(empty) == "<none>"


def test_concat_one() -> None:
    for one in ([1], {1}, (i for i in range(1, 2))):
        assert and_(one) == "1"


def test_concat_two() -> None:
    for two in ([1, 2], {1, 2}, (i for i in range(1, 3))):
        assert and_(two) == "1 and 2"


def test_concat_many() -> None:
    for many in ([1, 2, 3], {1, 2, 3}, (i for i in range(1, 4))):
        assert and_(many) == "1, 2 and 3"


@pytest.mark.parametrize(
    "word, expected",
    [
        ("t", "ts"),
        ("x", "xs"),
        ("f", "fs"),
        ("apple", "apples"),
        ("lemon", "lemons"),
        ("person", "people"),
        ("analysis", "analyses"),
        ("class", "classes"),
        ("watch", "watches"),
        ("category", "categories"),
        ("policy", "policies"),
        ("leaf", "leaves"),
        ("shelf", "shelves"),
        ("life", "lives"),
        ("knife", "knives"),
        ("echo", "echoes"),
        ("piano", "pianos"),
    ],
)
def test_pluralize(word: str, expected: str) -> None:
    print(word, pluralize(word))
    assert pluralize(word) == expected
