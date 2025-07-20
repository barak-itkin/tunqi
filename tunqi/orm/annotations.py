from __future__ import annotations

import re
from types import NoneType, UnionType
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Union

from tunqi.core.column import type_names

if TYPE_CHECKING:
    from tunqi.orm.model import Model

type PK = int
type Unique[T] = T
type Index[T] = T
type SchemaConstructor = Callable[[type[Model], Any, dict[str, Any]], dict[str, Any]]

RELATIONS = "FK", "OptionalFK", "Backref", "M2M"
RELATIONS_REGEX = re.compile(rf"(?<!\w)({'|'.join(RELATIONS)})\[(.*?)\](?!\w)")


def annotation_schema(name: str, annotation: Any) -> dict[str, Any]:
    cls, qualifiers = parse_qualifiers(name, annotation)
    if cls in type_names:
        type_name = type_names[cls]
        variant = qualifiers.pop("variant", None)
        if variant:
            type_name = f"{type_name}:{variant.pop('variant')}"
            qualifiers.update(variant)
        return {"type": type_name, **qualifiers}
    return {"type": "json", **qualifiers}


def parse_relations(annotations: dict[str, Any]) -> dict[str, Any]:
    relations: dict[str, Any] = {}
    for name, annotation in annotations.items():
        if isinstance(annotation, str):
            if RELATIONS_REGEX.search(annotation):
                relations[name] = annotation
        else:
            relation = parse_relation(name, annotation)
            if relation:
                relations[name] = annotation
    return relations


def parse_relation(name: str, annotation: Any) -> tuple[str, str, dict[str, Any]] | None:
    core, qualifiers = parse_qualifiers(name, annotation)
    origin, args = parse_generic(core)
    if not origin or origin.__name__ not in RELATIONS:
        return None
    if len(args) != 1:
        raise ValueError(f"invalid relation annotation for {name!r}: {annotation!r} (expected a single argument)")
    return origin.__name__, args[0].__name__, qualifiers


def parse_generic(annotation: Any) -> tuple[type | None, list[type]]:
    origin = getattr(annotation, "__origin__", None)
    if not origin:
        return None, []
    args = getattr(annotation, "__args__", None)
    if not args:
        return None, []
    return origin, args


def is_classvar(annotation: Any) -> bool:
    origin, _ = parse_generic(annotation)
    return origin is ClassVar


def parse_optional(annotation: Any) -> Any | None:
    if isinstance(annotation, UnionType):
        args = [arg for arg in annotation.__args__ if arg is not NoneType]
        return args[0]
    origin, args = parse_generic(annotation)
    if not origin or not args:
        return None
    if origin is not Union:
        return None
    args = [arg for arg in args if arg is not NoneType]
    if len(args) != 1:
        return None
    return args[0]


def parse_qualifiers(name: str, annotation: Any) -> tuple[Any, dict[str, Any]]:
    optional = parse_optional(annotation)
    if optional:
        core, qualifiers = parse_qualifiers(name, optional)
        return core, {"nullable": True, **qualifiers}
    origin, args = parse_generic(annotation)
    if not origin or not args:
        return annotation, {}
    if origin is Unique:
        if len(args) != 1:
            raise ValueError(f"invalid unique annotation for {name!r}: {annotation!r} (expected a single argument)")
        core, qualifiers = parse_qualifiers(name, args[0])
        return core, {"unique": True, **qualifiers}
    if origin is Index:
        if len(args) != 1:
            raise ValueError(f"invalid index annotation for {name!r}: {annotation!r} (expected a single argument)")
        core, qualifiers = parse_qualifiers(name, args[0])
        return core, {"index": True, **qualifiers}
    if hasattr(annotation, "__metadata__"):
        core, qualifiers = parse_qualifiers(name, origin)
        variant = annotation.__metadata__[0]
        if not isinstance(variant, dict) or "variant" not in variant:
            raise ValueError(
                f"invalid annotated metadata for {name!r}: {variant!r} (expected a dictionary with a 'variant' key)"
            )
        qualifiers["variant"] = variant
        return core, qualifiers
    return annotation, {}


def length(length: int) -> dict[str, Any]:
    return {"variant": "length", "length": length}
