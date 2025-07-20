from __future__ import annotations

from functools import cached_property, reduce
from operator import and_
from typing import TYPE_CHECKING, Any

from sqlalchemy import ColumnElement

from tunqi.core.expression import Expression
from tunqi.core.join import Joined, Joins, join, merge_joins
from tunqi.core.query import Query

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.table import Table


class Condition:

    def __init__(self, table: Table, expression: Expression | None = None, query: Query | None = None) -> None:
        self.table = table
        self.expression = expression
        self.query = query
        self._clause, self.joins = self._resolve()

    def __str__(self) -> str:
        if not self:
            return ""
        output: list[str] = []
        if self.expression:
            output.append(str(self.expression))
        if self.query:
            output.append(str(self.query))
        return " and ".join(output)

    def __repr__(self) -> str:
        if not self:
            return "<no condition>"
        return f"<condition {str(self)!r}>"

    def __bool__(self) -> bool:
        return self._clause is not None

    @classmethod
    def create(cls, table: Table, where: Expression | Query | None = None, /, **filters: Any) -> Condition:
        if not where:
            if not filters:
                return Condition(table)
            return cls(table, query=Query(filters))
        if isinstance(where, Query):
            if not filters:
                return cls(table, query=where)
            return cls(table, query=Query(where, filters))
        if not filters:
            return cls(table, expression=where)
        return cls(table, expression=where, query=Query(filters))

    @cached_property
    def clause(self) -> ColumnElement:
        if self._clause is None:
            raise ValueError("empty condition")
        return self._clause

    @cached_property
    def join_clause(self) -> Joined:
        return join(self.table, self.joins)

    def include_joins(self, joins: Joins) -> None:
        self.joins = merge_joins(self.joins, joins)

    def _resolve(self) -> tuple[ColumnElement | None, Joins]:
        if not self.expression and not self.query:
            return None, {}
        clauses: list[ColumnElement] = []
        all_joins: list[Joins] = []
        if self.query:
            clause, joins = self.query.resolve(self.table)
            clauses.append(clause)
            all_joins.append(joins)
        if self.expression:
            clause, joins = self.expression.resolve(self.table)
            clauses.append(clause)
            all_joins.append(joins)
        clause = reduce(and_, clauses)
        return clause, merge_joins(*all_joins)
