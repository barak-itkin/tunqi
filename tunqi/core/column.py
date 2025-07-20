from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, Callable

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Double,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.dialects.postgresql import JSONB

from tunqi.utils import and_, pluralize

if TYPE_CHECKING:
    from tunqi.core.table import Table

type ColumnConstructor = Callable[[Table, str, dict[str, Any]], Column | None]

constructors: dict[str, ColumnConstructor] = {}
type_names: dict[type, str] = {}


def column(name: str, type_: type | None = None) -> Callable[[ColumnConstructor], ColumnConstructor]:
    def decorator(constructor: ColumnConstructor) -> ColumnConstructor:
        constructors[name] = constructor
        if type_:
            type_names[type_] = name
        return constructor

    return decorator


def create_column(table: Table, name: str, schema: dict[str, Any]) -> Column | None:
    type_ = _require(schema, name, "type")
    if type_ not in constructors:
        raise ValueError(f"invalid column type: {type_!r} (available types are {and_(constructors)})")
    constructor = constructors[type_]
    return constructor(table, name, schema)


@column("fk")
def fk_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    table_name = _require(schema, name, "table")
    if schema.get("nullable"):
        on_delete = "SET NULL"
    else:
        on_delete = "CASCADE"
    table.database._fks[table.name][name] = table_name
    table.database._fks[table_name][table.plural] = table.name
    fk = ForeignKey(f"{table_name}.{table.pk_name}", ondelete=on_delete)
    return Column(name, Integer(), fk)


@column("backref")
def backref_column(table: Table, name: str, schema: dict[str, Any]) -> None:
    table_name = _require(schema, name, "table")
    table.database._fks[table.name][name] = table_name
    plural = pluralize(table_name)
    if name != plural:
        table.database._ignored_relations[table.name].add(plural)


@column("m2m")
def m2m_column(table: Table, name: str, schema: dict[str, Any]) -> None:
    table_name = _require(schema, name, "table")
    targets = sorted([table.name, table_name])
    link_table_name = "{}2{}".format(*targets)
    if link_table_name in table.database._tables:
        return
    table.database._ignored_tables.add(link_table_name)
    table.database.add_table(
        link_table_name,
        {
            "plural": link_table_name,
            "columns": {
                table.name: {
                    "type": "fk",
                    "table": table.name,
                    "index": True,
                },
                table_name: {
                    "type": "fk",
                    "table": table_name,
                    "index": True,
                },
            },
            "unique": [[table.name, table_name]],
        },
    )
    table.database._m2ms[table.name][name] = table_name, link_table_name
    table.database._m2ms[table_name][table.plural] = table.name, link_table_name
    table.database._ignored_relations[table.name].add(link_table_name)
    table.database._ignored_relations[table_name].add(link_table_name)


@column("boolean", bool)
def boolean_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    return Column(name, Boolean())


@column("integer", int)
def integer_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    return Column(name, Integer())


@column("double", float)
def double_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    return Column(name, Double())


@column("string", str)
def string_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    unique = schema.get("unique") or any(name in fields for fields in table.unique)
    if table.database.is_mysql and unique:
        column = f"{table.name}.{name}"
        raise ValueError(f"invalid column {column!r}: MySQL requires unique string columns to have length")
    return Column(name, Text())


@column("string:length")
def string_length_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    length: int = _require(schema, name, "length")
    return Column(name, String(length=length))


@column("binary", bytes)
def binary_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    return Column(name, LargeBinary())


@column("datetime", dt.datetime)
def datetime_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    if table.database.is_mysql:
        return Column(name, DATETIME(fsp=6))
    return Column(name, DateTime(timezone=True))


@column("json")
def json_column(table: Table, name: str, schema: dict[str, Any]) -> Column:
    if table.database.is_postgresql:
        return Column(name, JSONB())
    return Column(name, JSON())


def _require(schema: dict[str, Any], name: str, key: str) -> Any:
    value = schema.get(key)
    if not value:
        raise ValueError(f"column {name!r} must specify a {key}")
    return value
