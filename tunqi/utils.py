import re
from typing import Any, Iterable

import inflect

inflect_engine = inflect.engine()


def and_(items: Iterable[Any]) -> str:
    items = list(items)
    if len(items) == 0:
        return "<none>"
    if len(items) == 1:
        return str(items[0])
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    *items, last = items
    return f"{', '.join(str(item) for item in items)} and {last}"


def pluralize(word: str) -> str:
    if len(word) == 1:
        return f"{word}s"
    return inflect_engine.plural(word)


def to_snake_case(name: str) -> str:
    name = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return name.lower()
