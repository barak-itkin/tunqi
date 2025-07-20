from __future__ import annotations

from typing import TYPE_CHECKING, Any, NoReturn

from sqlalchemy import ColumnElement

from tunqi.core.functions_ import Function, functions
from tunqi.core.join import Joins, merge_joins
from tunqi.utils import and_

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.table import Table


class Expression:

    def __init__(self, alias: str | None = None, desc: bool | None = None) -> None:
        self._alias = alias
        self._desc = desc

    def __str__(self) -> str:
        raise NotImplementedError()  # pragma: no cover

    def __repr__(self) -> str:
        return f"<expression {str(self)!r}>"

    def __hash__(self) -> int:
        raise NotImplementedError()  # pragma: no cover

    def __eq__(self, other: Any) -> BinaryExpression:  # type: ignore[override]
        return BinaryExpression("eq", self, other)

    def __ne__(self, other: Any) -> BinaryExpression:  # type: ignore[override]
        return BinaryExpression("ne", self, other)

    def __gt__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("gt", self, other)

    def __lt__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("lt", self, other)

    def __ge__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("ge", self, other)

    def __le__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("le", self, other)

    def __add__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("add", self, other)

    def __sub__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("sub", self, other)

    def __mul__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("mul", self, other)

    def __truediv__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("div", self, other)

    def __mod__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("mod", self, other)

    def __pow__(self, other: Any, modulo: Any | None = None) -> BinaryExpression:
        if modulo is not None:
            raise NotImplementedError("power with modulo is not supported")
        return BinaryExpression("pow", self, other)

    def __pos__(self) -> UnaryExpression:
        return UnaryExpression("pos", self)

    def __neg__(self) -> UnaryExpression:
        return UnaryExpression("neg", self)

    def __invert__(self) -> UnaryExpression:
        return UnaryExpression("not", self)

    def __or__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("or", self, other)

    def __and__(self, other: Any) -> BinaryExpression:
        return BinaryExpression("and", self, other)

    def is_(self, other: Any) -> BinaryExpression:
        return BinaryExpression("is", self, other)

    def in_(self, container: Any) -> BinaryExpression:
        return BinaryExpression("in", self, container)

    def as_(self, alias: str) -> Expression:
        self._alias = alias
        return self

    def asc(self) -> Expression:
        self._desc = False
        return self

    def desc(self) -> Expression:
        self._desc = True
        return self

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        raise NotImplementedError()  # pragma: no cover

    def to_selector(self, table: Table, value: Expression | None = None) -> Selector:
        if value is None:
            value = self
        if isinstance(value, ColumnExpression):
            selector = Selector.create(table, value._selector)[0]
            if self._alias:
                selector.alias = self._alias
            if self._desc is not None:
                selector.desc = self._desc
            return selector
        clause, joins = value.resolve(table)
        return Selector(table, str(value), clause, joins, self._alias, self._desc)

    def _format(self, value: Any) -> str:
        if isinstance(value, Expression):
            output = [str(value)]
            if self._alias:
                output.append(f"AS {self._alias}")
            if self._desc is not None:
                output.append("DESC" if self._desc else "ASC")
            return " ".join(output)
        return repr(value)


class ColumnExpression(Expression):

    def __init__(self, selector: str) -> None:
        selector, alias, desc = Selector.parse_alias_and_order(selector)
        super().__init__(alias, desc)
        self._selector = selector

    def __str__(self) -> str:
        return self._selector

    def __hash__(self) -> int:
        return hash((self._selector, self._type))

    def __getattr__(self, name: str) -> ColumnExpression:
        return ColumnExpression(f"{self._selector}.{name}")

    def __call__(self, *args: Any) -> CallExpression:
        self._selector, name = self._selector.rsplit(".", 1)
        return CallExpression(self, name, *args)

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        selector = self.to_selector(table)
        return selector.clause, selector.joins


class UnaryExpression(Expression):

    def __init__(self, operator: str, operand: Expression) -> None:
        super().__init__(operand._alias, operand._desc)
        self._operator = operator
        self._operand = operand

    def __str__(self) -> str:
        return Function.get(self._operator).format(self._operand)

    def __hash__(self) -> int:
        return hash((self._operator, self._operand))

    def __getattr__(self, name: str) -> FunctionExpression:
        return FunctionExpression(self, name)

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        function = Function.get(self._operator)
        selector = self.to_selector(table, self._operand)
        return function(selector), selector.joins


class BinaryExpression(Expression):

    def __init__(self, operator: str, left: Expression, right: Any) -> None:
        super().__init__(left._alias, left._desc)
        self._operator = operator
        self._left = left
        self._right = right

    def __str__(self) -> str:
        left = self._format(self._left)
        right = self._format(self._right)
        return Function.get(self._operator).format(left, right)

    def __hash__(self) -> int:
        return hash((self._operator, self._left, self._right))

    def __getattr__(self, name: str) -> FunctionExpression:
        return FunctionExpression(self, name)

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        joins: list[Joins] = []
        left = self.to_selector(table, self._left)
        joins.append(left.joins)
        right = self._right
        if isinstance(self._right, Expression):
            right, right_joins = self._right.resolve(table)
            joins.append(right_joins)
        function = Function.get(self._operator)
        return function(left, right), merge_joins(*joins)


class FunctionExpression(Expression):

    def __init__(self, object: Expression, name: str) -> None:
        super().__init__(object._alias, object._desc)
        self._object = object
        self._name = name

    def __str__(self) -> str:
        return f"{self._object}.{self._name}"

    def __hash__(self) -> int:
        return hash((self._object, self._name))

    def __call__(self, *args: Any) -> CallExpression:
        return CallExpression(self._object, self._name, *args)

    def __getattr__(self, name: str) -> NoReturn:
        raise ValueError(f"function expression {self!r} must be invoked")

    def resolve(self, table: Table) -> NoReturn:
        raise ValueError(f"function expression {self!r} must be invoked")


class CallExpression(Expression):

    def __init__(self, object: Expression, name: str, *args: Any) -> None:
        if name not in functions:
            raise ValueError(f"invalid function {name!r} (available functions are {and_(functions)})")
        super().__init__(object._alias, object._desc)
        self._object = object
        self._name = name
        self._args = args

    def __str__(self) -> str:
        args = ", ".join(self._format(arg) for arg in self._args)
        return f"{self._object}.{self._name}({args})"

    def __hash__(self) -> int:
        return hash((self._object, self._name, *self._args))

    def __getattr__(self, name: str) -> FunctionExpression:
        return FunctionExpression(self, name)

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        selector = self.to_selector(table, self._object)
        args: list[ColumnElement] = []
        all_joins: list[Joins] = [selector.joins]
        for arg in self._args:
            if isinstance(arg, Expression):
                clause, joins = arg.resolve(table)
                args.append(clause)
                all_joins.append(joins)
            else:
                args.append(arg)
        return functions[self._name](selector, *args), merge_joins(*all_joins)


class ColumnRoot:

    def __repr__(self) -> str:
        return "<column root>"

    def __getattr__(self, name: str) -> ColumnExpression:
        return ColumnExpression(name)

    def __getitem__(self, key: str) -> ColumnExpression:
        return ColumnExpression(key)


c = ColumnRoot()


from tunqi.core.selector import Selector  # noqa: E402
