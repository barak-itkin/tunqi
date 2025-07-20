from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Callable, Concatenate, overload

from sqlalchemy import (
    Boolean,
    ColumnElement,
    Double,
    Integer,
    LargeBinary,
    String,
    cast,
    func,
    literal,
)
from sqlalchemy.dialects.mysql import BLOB
from sqlalchemy.dialects.postgresql import JSONB, JSONPATH

from tunqi.utils import and_

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.selector import Selector

type Callback[**P] = Callable[Concatenate[Selector, P], ColumnElement]


class Function:

    def __init__(self, name: str, callback: Callback, symbol: str | None = None, json_type: type | None = None) -> None:
        self.name = name
        self.callback = callback
        self.json_type = json_type
        self.signature = inspect.signature(callback)
        self.min_args, self.max_args = self._count_params()
        if self.min_args > 2 and symbol:
            raise ValueError(f"function {name!r} with {self.num_args} arguments cannot have a custom symbol")
        if not symbol:
            symbol = name.replace("_", " ")
        self.symbol = symbol

    def __str__(self) -> str:
        if self.symbol:
            return self.symbol
        return f"{self.name}({self.signature})"

    def __repr__(self) -> str:
        if self.symbol:
            function = repr(self.symbol)
        else:
            function = str(self)
        return f"<function {function}>"

    @classmethod
    def get(cls, name: str) -> Function:
        if name not in functions:
            raise ValueError(f"invalid function {name!r} (available functions are {and_(functions)})")
        return functions[name]

    @property
    def num_args(self) -> str:
        if not self.max_args:
            return f"{self.min_args}+"
        if self.min_args == self.max_args:
            return str(self.min_args)
        return f"{self.min_args}-{self.max_args}"

    def format(self, *args: Any) -> str:
        self._check_args(len(args))
        if self.symbol:
            if self.min_args == 1:
                if "{" in self.symbol:
                    return self.symbol.format(selector=args[0])
                return f"{self.symbol}{args[0]}"
            if "{" in self.symbol:
                return self.symbol.format(selector=args[0], value=args[1])
            return f"{args[0]} {self.symbol} {args[1]}"
        return f"{self.name}({', '.join(map(str, args))})"

    def __call__(self, selector: Selector, *args: Any) -> ColumnElement:
        selector.json_as(self.json_type)
        self._check_args(len(args) + 1)
        return self.callback(selector, *args)

    def _count_params(self) -> tuple[int, int | None]:
        min_args = 0
        max_args: int | None = 0
        for param in self.signature.parameters.values():
            if param.kind == param.VAR_POSITIONAL:
                max_args = None
            elif param.kind in (param.KEYWORD_ONLY, param.VAR_KEYWORD):
                raise ValueError("functions can't have keyword-only or variadic keyword parameters")
            else:
                if param.default is param.empty:
                    min_args += 1
                if max_args is not None:
                    max_args += 1
        return min_args, max_args

    def _check_args(self, num_args) -> None:
        if not self.min_args <= num_args <= self.max_args:
            raise TypeError(f"function {self} got {num_args} arguments, but it expects {self.num_args}")


functions: dict[str, Function] = {}


@overload
def function(
    symbol: str | None = None,
    /,
    *,
    name: str | None = None,
    json_type: type | None = None,
) -> Callable[[Callback], Callback]: ...


@overload
def function(
    function: Callback,
    /,
    *,
    symbol: str | None = None,
    name: str | None = None,
    json_type: type | None = None,
) -> Callback: ...


def function(
    arg: str | Callback | None = None,
    /,
    *,
    symbol: str | None = None,
    name: str | None = None,
    json_type: type | None = None,
) -> Callable[[Callback], Callback] | Callback:
    if arg is None or isinstance(arg, str):

        def decorator(target: Callback) -> Callback:
            return function(target, symbol=arg, name=name, json_type=json_type)

        return decorator
    if not name:
        name = arg.__name__
    functions[name] = Function(name, arg, symbol, json_type)
    return arg


@function("==")
def eq(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause == value


@function("!=")
def ne(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause != value


@function
def distinct_from(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.is_distinct_from(value)


@function(">")
def gt(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause > value


@function("<")
def lt(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause < value


@function(">=")
def ge(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause >= value


@function("<=")
def le(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause <= value


@function(name="is")
def is_(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.is_(value)


@function
def is_not(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.isnot(value)


@function(name="in")
def in_(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.in_(value)


@function()
def not_in(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.notin_(value)


@function("containing")
def contains(selector: Selector, value: Any) -> ColumnElement:
    if selector.column is not None and isinstance(selector.column.type, JSONB):
        return selector.clause.op("@>")(literal([value], type_=JSONB))
    return selector.clause.contains(value)


@function("having")
def has(selector: Selector, value: Any) -> ColumnElement:
    segments: list[str] = []
    for segment in value.split("."):
        segments.append(f"[{segment}]" if segment.isdigit() else f".{segment}")
    path = f"${''.join(segments)}"
    if selector.table.database.is_sqlite:
        return func.json_type(selector.clause, path).is_not(None)
    elif selector.table.database.is_postgresql:
        return func.jsonb_path_exists(selector.clause, cast(path, JSONPATH))
    else:  # MySQL
        return func.json_contains_path(selector.clause, "one", path)


@function("starting with")
def startswith(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.startswith(value)


@function("ending with")
def endswith(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.endswith(value)


@function
def like(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.like(value)


@function
def not_like(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.notlike(value)


@function("matching")
def matches(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause.regexp_match(value)


@function("||", json_type=str)
def concat_(selector: Selector, *values: Any) -> ColumnElement:
    return func.concat(*values)


@function(json_type=str)
def length(selector: Selector) -> ColumnElement:
    return func.length(selector.clause)


@function
def boolean(selector: Selector) -> ColumnElement:
    if selector.json_path:
        return selector.clause.as_boolean()
    return func.cast(selector.clause, Boolean)


@function
def integer(selector: Selector) -> ColumnElement:
    if selector.json_path:
        return selector.clause.as_integer()
    return func.cast(selector.clause, Integer)


@function
def double(selector: Selector) -> ColumnElement:
    if selector.json_path:
        return selector.clause.as_float()
    return func.cast(selector.clause, Double)


@function
def string(selector: Selector) -> ColumnElement:
    if selector.json_path:
        return selector.clause.as_string()
    return func.cast(selector.clause, String)


@function(json_type=str)
def binary(selector: Selector) -> ColumnElement:
    if selector.table.database.is_mysql:
        return func.cast(selector.clause, BLOB)
    return func.cast(selector.clause, LargeBinary)


@function("+")
def add(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause + value


@function("-")
def sub(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause - value


@function("*")
def mul(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause * value


@function("/")
def div(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause / value


@function("%")
def mod(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause % value


@function("**")
def pow(selector: Selector, value: Any) -> ColumnElement:
    return selector.clause**value


@function("+")
def pos(selector: Selector) -> ColumnElement:
    return func.abs(selector.clause)


@function("-")
def neg(selector: Selector) -> ColumnElement:
    return -selector.clause
