from __future__ import annotations

import datetime as dt
from functools import reduce
from operator import and_, or_
from typing import TYPE_CHECKING, Any

from sqlalchemy import ColumnElement

from tunqi.core.expression import Expression
from tunqi.core.functions_ import functions
from tunqi.core.join import Joins, merge_joins
from tunqi.core.selector import Selector

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.table import Table


class Query:

    __slots__ = "operands", "operator", "negate"

    def __init__(self, *operands: Query | dict[str, Any], operator: str = "and", negate: bool = False) -> None:
        self.operands = list(operands)
        self.operator = operator
        self.negate = negate

    def __str__(self) -> str:
        clauses: list[str] = []
        for operand in self.operands:
            if isinstance(operand, Query):
                clause = str(operand)
                if not clause.startswith("not") and (" and " in clause or " or " in clause):
                    clause = f"({clause})"
                clauses.append(clause)
                continue
            for key, value in operand.items():
                clauses.append(self._format_filter(key, value))
        if len(clauses) > 1:
            output = f" {self.operator} ".join(clauses)
        else:
            output = clauses[0]
        if self.negate:
            if " and " in output or " or " in output:
                output = f"not ({output})"
            else:
                output = f"not {output}"
        return output

    def __repr__(self) -> str:
        return f"<query {str(self)!r}>"

    def __invert__(self) -> Query:
        return type(self)(*self.operands, operator=self.operator, negate=not self.negate)

    def __or__(self, other: Any) -> Query:
        if not isinstance(other, Query):
            return NotImplemented
        return type(self)(self, other, operator="or")

    def __and__(self, other: Any) -> Query:
        if not isinstance(other, Query):
            return NotImplemented
        return type(self)(self, other, operator="and")

    def resolve(self, table: Table) -> tuple[ColumnElement, Joins]:
        clauses: list[ColumnElement] = []
        all_joins: list[Joins] = []
        for operand in self.operands:
            if isinstance(operand, Query):
                clause, joins = operand.resolve(table)
                clauses.append(clause)
                all_joins.append(joins)
                continue
            for key, value in operand.items():
                clause, joins = self._resolve_filter(table, key, value)
                clauses.append(clause)
                all_joins.append(joins)
        operator = and_ if self.operator == "and" else or_
        clause = reduce(operator, clauses)
        if self.negate:
            clause = ~clause
        return clause, merge_joins(*all_joins)

    def _format_filter(self, key: str, value: Any) -> str:
        key = key.replace("__", ".")
        if isinstance(value, Expression):
            value = str(value)
        else:
            value = repr(value)
        if "." in key:
            selector, function_name = key.rsplit(".", 1)
            if function_name in functions:
                function = functions[function_name]
                return function.format(selector, value)
        return f"{key} == {value}"

    def _resolve_filter(self, table: Table, key: str, value: Any) -> tuple[ColumnElement, Joins]:
        key = key.replace("__", ".")
        if "." not in key:
            selector = Selector.from_column(table, key)
            return selector.clause == value, selector.joins
        selector_name, function_name = key.rsplit(".", 1)
        if function_name in functions and functions[function_name].min_args == 2:
            function = functions[function_name]
        else:
            function = functions["eq"]
            selector_name = key
        selector = Selector.create(table, selector_name)[0]
        joins = selector.joins
        if isinstance(value, dt.datetime):
            value = value.astimezone(dt.UTC)
        if isinstance(value, Expression):
            value, value_joins = value.resolve(table)
            joins = merge_joins(joins, value_joins)
        if not selector.json_path:
            return function(selector, value), joins
        if function.name == "ne":
            function = functions["distinct_from"]
        selector.json_as(type(value))
        clause = function(selector, value)
        if selector.column is not None and function.name != "has":
            column = Selector.from_column(table, selector.column.name)
            clause = functions["has"](column, selector.json_path) & clause
        return clause, joins


def q(operator: str = "and", /, **filter: Any) -> Query:
    return Query(filter, operator=operator)
