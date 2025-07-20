from __future__ import annotations

from functools import cached_property
from itertools import product
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

import sqlalchemy
from sqlalchemy import (
    JSON,
    Column,
    ColumnElement,
    Delete,
    Index,
    Insert,
    Integer,
    Select,
    UniqueConstraint,
    Update,
    exists,
    func,
    select,
    tuple_,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import Insert as PostgreSQLInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tunqi.core.column import create_column
from tunqi.core.condition import Condition
from tunqi.core.selector import Selectors
from tunqi.utils import and_, pluralize

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.database import Database

type Row = dict[str, Any]
type Relations = dict[str, list[Table]]

ROW_NUMBER = "__row_number__"


class Table:

    pk_name: ClassVar[str] = "pk"

    def __init__(self, database: Database, name: str, schema: dict[str, Any]) -> None:
        self.database = database
        self.name = name
        self.schema = schema
        self.plural: str = schema["plural"] if "plural" in schema else pluralize(name)
        self.unique: list[tuple[str, ...]] = schema["unique"] if "unique" in schema else []
        self.table, self.pk = self._create_table()

    def __str__(self) -> str:
        return f"table {self.name!r}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @cached_property
    def relations(self) -> Relations:
        relations: Relations = {}
        for column_name, table_name in self.database._fks[self.name].items():
            self._add_fk_relation(relations, column_name, table_name)
        for column_name, (table_name, link_table_name) in self.database._m2ms[self.name].items():
            self._add_m2m_relation(relations, column_name, table_name, link_table_name)
        for column in self.database._ignored_relations[self.name]:
            relations.pop(column, None)
        return relations

    def exists(self, condition: Condition) -> Select:
        statement = select(self.table)
        if condition:
            statement = statement.where(condition.clause)
            if condition.joins:
                statement = statement.select_from(condition.join_clause)
        statement = select(exists(statement))
        return statement

    def count(self, selectors: Selectors, condition: Condition) -> Select:
        if selectors:
            statement = select(*selectors.clauses).distinct()
        else:
            statement = select(self.pk)
        if condition:
            if condition.joins:
                condition.include_joins(selectors.joins)
                subquery = select(*selectors.pks).where(condition.clause).select_from(condition.join_clause).distinct()
                statement = statement.where(tuple_(*selectors.pks).in_(subquery)).select_from(selectors.join_clause)
            else:
                statement = statement.where(condition.clause)
        return select(func.count("*")).select_from(statement.subquery())

    def insert(self, rows: list[Row], on_conflict: list[str], update: Selectors, return_pks: bool = False) -> Insert:
        if self.database.is_mysql:
            # Make sure the PK is present in the INSERT statement (for MySQL's ON CONFLICT DO NOTHING to work).
            for row in rows:
                row[self.pk_name] = None
        statement = self._insert_on_conflict(on_conflict, update)
        if return_pks and not self.database.is_mysql:
            statement = statement.returning(self.pk)
        if rows:
            statement = statement.values(rows)
        return statement

    def update(self, condition: Condition) -> Update:
        if not condition:
            return self.table.update()
        if not condition.joins:
            return self.table.update().where(condition.clause)
        subquery = self._change_subquery(condition)
        return self.table.update().where(self.pk.in_(subquery))

    def delete(self, condition: Condition) -> Delete:
        if not condition:
            return self.table.delete()
        if not condition.joins:
            return self.table.delete().where(condition.clause)
        subquery = self._change_subquery(condition)
        return self.table.delete().where(self.pk.in_(subquery))

    def select(
        self,
        selectors: Selectors,
        condition: Condition,
        limit: int | None = None,
        offset: int | None = None,
        order: Selectors | None = None,
    ) -> Select:
        if selectors:
            statement = select(*selectors.select_terms())
        else:
            statement = select(self.table)
        # If JOINs are needed for the WHERE clause, we use a subquery to guarantee distinct rows.
        condition.include_joins(selectors.joins)
        if order:
            condition.include_joins(order.joins)
        if condition.joins:
            # statement = self._select_subquery(statement, selectors, condition, order)
            statement = statement.select_from(condition.join_clause)
            statement = statement.group_by(*selectors.pks)
            if condition:
                statement = statement.where(condition.clause)
            if order:
                sort_terms: list[ColumnElement] = []
                for selector in order.selectors:
                    if selector.desc:
                        sort_term = func.max(selector.clause).label(selector.alias).desc()
                    else:
                        sort_term = func.min(selector.clause).label(selector.alias).asc()
                    sort_terms.append(sort_term)
                statement = statement.order_by(*sort_terms)
        else:
            if condition:
                statement = statement.where(condition.clause)
            if order:
                statement = statement.order_by(*order.sort_terms())
        if limit:
            statement = statement.limit(limit)
        if offset:
            statement = statement.offset(offset)
        return statement

    def link(self, m2m_name: str, sources: Iterable[int], targets: Iterable[int]) -> Insert:
        if m2m_name not in self.database._m2ms[self.name]:
            raise ValueError(
                f"table {self.name!r} has no many-to-many relation {m2m_name!r} "
                f"(available many-to-many relations are {and_(self.database._m2ms[self.name])})"
            )
        target_table_name, link_table_name = self.database._m2ms[self.name][m2m_name]
        link_table = self.database._tables[link_table_name]
        rows = [{self.name: source, target_table_name: target} for source, target in product(sources, targets)]
        update = Selectors.resolve(link_table, False)
        statement = link_table.insert(rows, on_conflict=[self.name, target_table_name], update=update, return_pks=False)
        return statement

    def unlink(self, m2m_name: str, sources: Iterable[int], targets: Iterable[int]) -> Delete:
        if m2m_name not in self.database._m2ms[self.name]:
            raise ValueError(
                f"table {self.name!r} has no many-to-many relation {m2m_name!r} "
                f"(available many-to-many relations are {and_(self.database._m2ms[self.name])})"
            )
        target_table_name, link_table_name = self.database._m2ms[self.name][m2m_name]
        if target_table_name not in self.database._tables:
            raise ValueError(
                f"table {target_table_name!r} doesn't exist (available tables are {and_(self._available_tables())})"
            )
        target_table = self.database._tables[target_table_name]
        link_table = self.database._tables[link_table_name]
        pks = list(product(sources, targets))
        columns = [link_table.table.columns[self.name], link_table.table.columns[target_table.name]]
        statement = link_table.table.delete().where(tuple_(*columns).in_(pks))
        return statement

    def _create_table(self) -> tuple[sqlalchemy.Table, Column]:
        columns: dict[str, Column] = {}
        pk = Column(self.pk_name, Integer(), primary_key=True, autoincrement=True)
        columns[self.pk_name] = pk
        indexes: list[Index] = []
        for column_name, column_schema in self.schema.get("columns", {}).items():
            nullable = column_schema.get("nullable", False)
            index = column_schema.get("index", False)
            unique = column_schema.get("unique", False)
            column = create_column(self, column_name, column_schema)
            if column is None:
                continue
            column.nullable = nullable
            column.index = index
            column.unique = unique
            # Regular indices don't make sense for JSON columns, so we change them to GIN indices.
            if isinstance(column.type, (JSON, JSONB)) and index:
                column.index = False
                if self.database.is_postgresql:
                    indexes.append(Index(f"ix_{self.name}_{column_name}", column, postgresql_using="gin"))
            columns[column_name] = column
        # Unique together constraints reference multiple columns, so they are stored separately.
        constraints = [UniqueConstraint(*fields) for fields in self.unique]
        table = sqlalchemy.Table(self.name, self.database.metadata, *columns.values(), *indexes, *constraints)
        return table, pk

    def _add_fk_relation(self, relations: Relations, column_name: str, table_name: str) -> None:
        if table_name not in self.database._tables:
            fk = f"{self.name}.{column_name}"
            raise ValueError(
                f"table {table_name!r} referenced by foreign key {fk!r} doesn't exist "
                f"(available tables are {and_(self._available_tables())})"
            )
        relations[column_name] = [self.database._tables[table_name]]

    def _add_m2m_relation(self, relations: Relations, column_name: str, table_name: str, link_table_name: str) -> None:
        if table_name not in self.database._tables:
            m2m = f"{self.name}.{column_name}"
            raise ValueError(
                f"table {table_name!r} referenced by many-to-many {m2m!r} doesn't exist "
                f"(available tables are {and_(self._available_tables())})"
            )
        relations[column_name] = [self.database._tables[link_table_name], self.database._tables[table_name]]

    def _insert_on_conflict(self, on_conflict: list[str], update: Selectors) -> Insert:
        if not on_conflict:
            return self.table.insert()
        column_names = [selector.column.name for selector in update.selectors if selector.column is not None]
        if self.pk_name in column_names:
            column_names.remove(self.pk_name)
        if self.database.is_mysql:
            mysql_statement = mysql_insert(self.table)
            # MySQL doesn't support ON DUPLICATE KEY DO NOTHING, so we assign the PK to itself.
            if not column_names:
                columns = {self.pk.name: self.pk}
            else:
                columns = {name: getattr(mysql_statement.inserted, name) for name in column_names}
            return mysql_statement.on_duplicate_key_update(**columns)
        statement: SQLiteInsert | PostgreSQLInsert
        if self.database.is_sqlite:
            statement = sqlite_insert(self.table)
        else:  # PostgreSQL
            statement = pg_insert(self.table)
        if not column_names:
            return statement.on_conflict_do_nothing(index_elements=on_conflict)
        columns = {name: getattr(statement.excluded, name) for name in column_names}
        return statement.on_conflict_do_update(index_elements=on_conflict, set_=columns)

    def _change_subquery(self, condition: Condition) -> Select:
        subquery = select(self.pk).where(condition.clause).select_from(condition.join_clause)
        # MySQL doesn't support changing a table and selecting from it at the same time, so we have to double-wrap the
        # subquery.
        if self.database.is_mysql:
            wrapped = select(subquery.subquery().c[self.pk.name]).subquery()
            subquery = select(wrapped.c[self.pk.name])
        return subquery

    def _available_tables(self) -> list[str]:
        return [name for name in self.database._tables if name not in self.database._ignored_tables]

    def _available_selectors(self) -> list[str]:
        fields = [column.name for column in self.table.columns]
        fields.extend(self.relations)
        return fields
