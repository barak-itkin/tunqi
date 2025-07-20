from __future__ import annotations

import collections
from functools import cached_property
from typing import TYPE_CHECKING, ClassVar, Iterable

from sqlalchemy import JSON, Column, ColumnElement
from sqlalchemy.dialects.postgresql import JSONB

from tunqi.core.functions_ import Function, functions
from tunqi.core.join import Joined, Joins, join, merge_joins
from tunqi.utils import and_

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.table import Table

type SelectorTypes = bool | str | Expression | Iterable[str | Expression] | None


class Selector:

    alias_delimiter: ClassVar[str] = ":"
    __slots__ = "table", "selector", "clause", "joins", "alias", "desc", "column", "json_path"

    def __init__(
        self,
        table: Table,
        selector: str,
        clause: ColumnElement,
        joins: Joins | None = None,
        alias: str | None = None,
        desc: bool | None = None,
        column: Column | None = None,
        json_path: str | None = None,
    ) -> None:
        if joins is None:
            joins = {}
        if alias is None:
            alias = selector
        self.table = table
        self.selector = selector
        self.clause = clause
        self.joins = joins
        self.alias = alias
        self.desc = desc
        self.column = column
        self.json_path = json_path

    def __str__(self) -> str:
        clause = self.clause
        if self.alias:
            clause = self.clause.label(self.alias)
        if self.desc is not None:
            clause = clause.desc() if self.desc else clause.asc()
        return self.table.database._format_clause(clause)

    def __repr__(self) -> str:
        return f"<selector {self.selector!r}: {str(self)}>"

    @classmethod
    def parse_alias_and_order(cls, selector: str) -> tuple[str, str, bool | None]:
        alias = selector
        desc: bool | None = None
        if selector.startswith("-"):
            selector, desc = selector.removeprefix("-"), True
        if selector.startswith("+"):
            selector, desc = selector.removeprefix("+"), False
        if cls.alias_delimiter in selector:
            selector, alias = selector.split(cls.alias_delimiter, 1)
        return selector, alias, desc

    @classmethod
    def from_column(cls, table: Table, name: str) -> Selector:
        columns = {column.name: column for column in table.table.columns}
        if name not in columns:
            raise ValueError(f"{table} has no column {name!r} (available columns are {and_(columns)})")
        column = columns[name]
        return cls(table, name, column, column=column)

    @classmethod
    def create(cls, table: Table, selector: str) -> list[Selector]:
        selector, alias, desc = cls.parse_alias_and_order(selector)
        segments = selector.split(".")
        table, joins = cls._traverse_joins(table, segments)
        if not segments:
            selectors: list[Selector] = []
            for column in table.table.columns:
                column_alias = f"{alias}.{column.name}" if alias else None
                selectors.append(Selector(table, selector, column, joins, column_alias, desc, column))
            return selectors
        clause, column, json_path = cls._traverse_path(table, segments)
        return [Selector(table, selector, clause, joins, alias, desc, column, json_path)]

    def json_as(self, json_type: type | None) -> None:
        if not self.json_path:
            return
        if json_type is bool:
            self.clause = self.clause.as_boolean()
        elif json_type is int:
            self.clause = self.clause.as_integer()
        elif json_type is float:
            self.clause = self.clause.as_float()
        elif json_type is str:
            self.clause = self.clause.as_string()

    @classmethod
    def _traverse_joins(cls, table: Table, segments: list[str]) -> tuple[Table, Joins]:
        joins: Joins = collections.defaultdict(list)
        while segments:
            if segments[0] not in table.relations:
                break
            for related_table in table.relations[segments.pop(0)]:
                if related_table not in joins[table]:
                    joins[table].append(related_table)
                table = related_table
        return table, joins

    @classmethod
    def _traverse_path(cls, table: Table, segments: list[str]) -> tuple[ColumnElement, Column, str | None]:
        column_name = segments.pop(0)
        if column_name not in table.table.columns:
            selectors = table._available_selectors()
            raise ValueError(f"{table} has no column {column_name!r} (available selectors are {and_(selectors)})")
        column = table.table.columns[column_name]
        if not segments:
            return column, column, None
        clause = cls._traverse_functions(table, segments, column)
        if not segments:
            return clause, column, None
        return cls._traverse_json_path(table, segments, column, clause)

    @classmethod
    def _traverse_functions(cls, table: Table, segments: list[str], column: Column) -> ColumnElement:
        selector = cls(table, column.name, column, column=column)
        while segments:
            if segments[0] not in functions:
                break
            function = Function.get(segments.pop(0))
            selector.clause = function(selector)
        return selector.clause

    @classmethod
    def _traverse_json_path(
        cls,
        table: Table,
        segments: list[str],
        column: Column,
        clause: ColumnElement,
    ) -> tuple[ColumnElement, Column, str | None]:
        if not isinstance(column.type, JSON | JSONB):
            column_name = f"{table.name}.{column.name}"
            message = f"column {column_name!r} is not a JSON column"
            if len(segments) == 1:
                message += f" and {segments[0]!r} is not a function (available functions are {and_(functions)})"
            raise ValueError(message)
        json_path: str | None = ""
        for segment in segments:
            if segment in functions and functions[segment].min_args == 1:
                selector = cls(table, f"{column.name}{json_path or ""}", clause, column=column, json_path=json_path)
                clause = functions[segment](selector)
                json_path = None
            else:
                clause = clause[int(segment) if segment.isdigit() else segment]
                if json_path is not None:
                    json_path += f".{segment}"
        return clause, column, json_path and json_path.strip(".")


class Selectors:

    def __init__(self, table: Table, selectors: list[Selector]) -> None:
        self.table = table
        self.selectors = selectors

    def __str__(self) -> str:
        if not self:
            return ""
        return ", ".join(str(selector) for selector in self.selectors)

    def __repr__(self) -> str:
        if not self:
            return "<no selectors>"
        return f"<selectors {str(self)!r}>"

    def __bool__(self) -> bool:
        return len(self.selectors) > 0

    @classmethod
    def resolve(cls, table: Table, selectors: SelectorTypes, only_columns: bool = False) -> Selectors:
        if not selectors:
            return Selectors(table, [])
        if selectors is True:
            selectors = [column.name for column in table.table.columns]
            only_columns = True
        elif isinstance(selectors, str | Expression):
            selectors = [selectors]
        if only_columns:
            return cls(table, [Selector.from_column(table, str(name)) for name in selectors])
        resolved: list[Selector] = []
        for selector in selectors:
            if isinstance(selector, str):
                resolved.extend(Selector.create(table, selector))
            else:
                resolved.append(selector.to_selector(table))
        return cls(table, resolved)

    @cached_property
    def pks(self) -> list[Column]:
        if not self.selectors:
            return [self.table.pk]
        return list({selector.table.pk for selector in self.selectors})

    @cached_property
    def clauses(self) -> list[ColumnElement]:
        return [selector.clause for selector in self.selectors]

    @cached_property
    def joins(self) -> Joins:
        return merge_joins(*(selector.joins for selector in self.selectors))

    @cached_property
    def join_clause(self) -> Joined:
        return join(self.table, self.joins)

    def select_terms(self) -> list[ColumnElement]:
        return [selector.clause.label(selector.alias) for selector in self.selectors]

    def sort_terms(self) -> list[ColumnElement]:
        sort_terms: list[ColumnElement] = []
        for selector in self.selectors:
            clause = selector.clause
            if selector.desc is not None:
                clause = clause.desc() if selector.desc else clause.asc()
            sort_terms.append(clause)
        return sort_terms


from tunqi.core.expression import Expression  # noqa: E402
