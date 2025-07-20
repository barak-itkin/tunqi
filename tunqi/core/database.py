from __future__ import annotations

import collections
import datetime as dt
import json
import pathlib
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar, Token
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    Iterator,
    Mapping,
    cast,
    overload,
)

from sqlalchemy import (
    ClauseElement,
    CursorResult,
    Executable,
    MetaData,
    Update,
    event,
    make_url,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncTransaction,
    create_async_engine,
)
from sqlalchemy.sql.compiler import SQLCompiler
from srlz import Serialization

from tunqi.audit import AuditEvent, AuditEventBase, Auditor
from tunqi.core.condition import Condition
from tunqi.core.expression import Expression
from tunqi.core.migration import Migration
from tunqi.core.query import Query
from tunqi.core.selector import Selectors, SelectorTypes
from tunqi.core.table import Row, Table
from tunqi.errors import AlreadyExistsError, DoesNotExistError
from tunqi.utils import and_

SQLITE_PARAMETER = re.compile(r"\?")
POSTGRESQL_PARAMETER = re.compile(r"\$(\d+)(::[A-Z ]+)?")
MYSQL_PARAMETER = re.compile(r"%s")
SQLITE_UNIQUE_ERROR = re.compile(r"UNIQUE constraint failed: (.*)")
POSTGRESQL_UNIQUE_ERROR = re.compile(r"DETAIL:\s*Key \((.*)\)=\((.*)\) already exists")
MYSQL_UNIQUE_ERROR = re.compile(r"Duplicate entry '(.*)' for key '(.*)'")


class Database:

    default_serialization: ClassVar[Serialization] = Serialization()
    default_database: ClassVar[Database | None] = None
    active_database: ClassVar[ContextVar[Database | None]] = ContextVar("active_database", default=None)
    active_connection: ClassVar[ContextVar[AsyncConnection | None]] = ContextVar("active_connection", default=None)
    active_transaction: ClassVar[ContextVar[AsyncTransaction | None]] = ContextVar("active_transaction", default=None)

    def __init__(
        self,
        url: str,
        *,
        default: bool = False,
        serialization: Serialization | None = None,
        auditor: Auditor | None = None,
    ) -> None:
        if serialization is None:
            serialization = self.default_serialization
        self.serialization = serialization
        self.url = make_url(url).render_as_string(hide_password=True)
        self.engine = create_async_engine(self._url_with_driver(url))
        if self.is_sqlite:
            event.listens_for(self.engine.sync_engine, "connect")(self._configure_sqlite)
        self.metadata = MetaData()
        self.auditor = auditor
        self._tables: dict[str, Table] = {}
        self._fks: dict[str, dict[str, str]] = collections.defaultdict(dict)
        self._m2ms: dict[str, dict[str, tuple[str, str]]] = collections.defaultdict(dict)
        self._ignored_tables: set[str] = set()
        self._ignored_relations: dict[str, set[str]] = collections.defaultdict(set)
        self._token: Token[Database | None] | None = None
        if default:
            self.set_default()

    def __str__(self) -> str:
        return f"database at {self.url!r}"

    def __repr__(self) -> str:
        return f"<{self}>"

    def __enter__(self) -> None:
        self._token = self.active_database.set(self)

    def __exit__(self, *_) -> None:
        if self._token:
            self.active_database.reset(self._token)

    @property
    def is_sqlite(self) -> bool:
        return self.url.startswith("sqlite")

    @property
    def is_postgresql(self) -> bool:
        return self.url.startswith("postgresql")

    @property
    def is_mysql(self) -> bool:
        return self.url.startswith("mysql")

    @classmethod
    def get(cls) -> Database:
        current = cls.active_database.get()
        if current:
            return current
        if cls.default_database:
            return cls.default_database
        raise RuntimeError("no active nor default database")

    def set_default(self) -> None:
        self.__class__.default_database = self

    def clear_default(self) -> None:
        if self is self.default_database:
            self.__class__.default_database = None

    @contextmanager
    def audit(self, auditor: Auditor) -> Iterator[None]:
        prev_auditor, self.auditor = self.auditor, auditor
        try:
            yield
        finally:
            self.auditor = prev_auditor

    @overload
    def serialize(self, data: Row) -> Row: ...

    @overload
    def serialize(self, data: Iterable[Row]) -> list[Row]: ...

    def serialize(self, data: Row | Iterable[Row]) -> Row | Iterable[Row]:
        if isinstance(data, dict):
            serialized: Row = {}
            for key, value in data.items():
                if isinstance(value, dt.datetime):
                    serialized[key] = value.astimezone(dt.UTC)
                elif isinstance(value, list | dict):
                    serialized[key] = self.serialization.serialize(value, key)
                else:
                    serialized[key] = value
            return serialized
        return [self.serialize(item) for item in data]

    def deserialize(self, data: Row) -> Row:
        deserialized = {}
        for key, value in data.items():
            if isinstance(value, dt.datetime):
                deserialized[key] = value.replace(tzinfo=dt.UTC).astimezone()
            else:
                deserialized[key] = self.serialization.deserialize(value)
        return deserialized

    def get_table(self, name: str) -> Table:
        if name not in self._tables:
            raise ValueError(f"table {name!r} doesn't exist (available tables are {and_(self._tables)})")
        return self._tables[name]

    def add_table(self, name: str, schema: dict[str, Any]) -> Table:
        if name in self._tables:
            if self._tables[name].schema == schema:
                return self._tables[name]
            raise ValueError(f"table {name!r} already exists")
        table = Table(self, name, schema)  # type: ignore
        self._tables[name] = table
        return table

    def remove_table(self, name: str) -> Table:
        if name not in self._tables:
            raise ValueError(f"table {name!r} doesn't exist (available tables are {and_(self._tables)})")
        return self._tables.pop(name)

    async def create_database(self, name: str) -> Database:
        url = make_url(self.engine.url)
        url = url.set(database=name, drivername=self.engine.url.drivername.split("+")[0])
        async with self.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            await connection.execute(text(f"CREATE DATABASE {name}"))
            if self.is_mysql:
                await connection.execute(text(f"GRANT ALL PRIVILEGES ON {name}.* TO '{url.username}'@'%'"))
                await connection.execute(text("FLUSH PRIVILEGES"))
        return Database(url.render_as_string(hide_password=False))

    async def drop_database(self, name: str) -> None:
        async with self.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            await connection.execute(text(f"DROP DATABASE {name}"))
        await self.stop()

    async def create_tables(self, table_names: Iterable[str] | None = None) -> None:
        tables = self._get_relevant_tables(table_names)
        async with self.engine.begin() as connection:
            await connection.run_sync(self.metadata.create_all, tables=[table.table for table in tables])

    async def drop_tables(self, table_names: Iterable[str] | None = None) -> None:
        tables = self._get_relevant_tables(table_names)
        async with self.engine.begin() as connection:
            await connection.run_sync(self.metadata.drop_all, tables=[table.table for table in tables])

    async def make_migrations(
        self,
        migrations_directory: str | pathlib.Path,
        table_names: Iterable[str] | None = None,
    ) -> None:
        tables = self._get_relevant_tables(table_names)
        migrations_directory = pathlib.Path(migrations_directory)
        migration = Migration(self.metadata, migrations_directory, [table.name for table in tables])
        async with self.engine.connect() as connection:
            await connection.run_sync(migration.make_migrations)

    async def migrate(self, migrations_directory: str | pathlib.Path) -> None:
        migrations_directory = pathlib.Path(migrations_directory)
        migration = Migration(self.metadata, migrations_directory)
        async with self.engine.connect() as connection:
            await connection.run_sync(migration.migrate)

    async def stop(self) -> None:
        with self._audit("stop"):
            self.clear_default()
            await self.engine.dispose()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        # If a connection already exists, this is a nested call and we return the existing connection.
        connection = self.active_connection.get()
        if connection is not None:
            yield connection
            return
        # Otherwise, we create a new connection and store it for nested calls.
        with self._audit("connect") as event:
            async with self.engine.connect() as connection:
                event.set(connection_id=id(connection), connection_uid=str(uuid.uuid4()))
                token = self.active_connection.set(connection)
                try:
                    yield connection
                finally:
                    self.active_connection.reset(token)

    @asynccontextmanager
    async def transaction(self, nested: bool = False) -> AsyncIterator[AsyncTransaction]:
        # If a transaction already exists, this is a nested call and we return the existing transaction.
        transaction = self.active_transaction.get()
        if transaction is not None and not nested:
            try:
                yield transaction
                return
            except Exception:
                # If we returned an existing transaction in a nested call and it failed, we roll it back here, in case
                # the error doesn't reach the outermost context where the transaction was created.
                with self._audit("rollback"):
                    await transaction.rollback()
                raise
        # Otherwise, we create a new transaction and store it for nested calls.
        async with self.connection() as connection:
            if nested:
                with self._audit("begin_nested") as event:
                    transaction = await connection.begin_nested()
                    event.set(transaction_id=id(transaction))
                    async with self._transaction(transaction) as transaction:
                        yield transaction
            else:
                with self._audit("begin") as event:
                    transaction = await connection.begin()
                    event.set(transaction_id=id(transaction))
                    async with self._transaction(transaction) as transaction:
                        yield transaction

    @asynccontextmanager
    async def execute(
        self,
        statement: str | Executable,
        values: Mapping[str, Any] | None = None,
        *,
        autocommit: bool = False,
    ) -> AsyncIterator[CursorResult]:
        async with self.connection() as connection:
            with self._audit("execute") as event:
                if isinstance(statement, str):
                    statement = text(statement)
                event.set_statement(statement, values)
                cursor = await connection.execute(statement, values)
                yield cursor
                if autocommit and not self.active_transaction.get():
                    with self._audit("autocommit"):
                        await connection.commit()

    async def exists(self, table_name: str, *, where: Expression | Query | None = None, **query: Any) -> bool:
        with self._audit("exists") as event:
            table = self.get_table(table_name)
            condition = Condition.create(table, where, **query)
            statement = table.exists(condition)
            async with self.execute(statement) as cursor:
                result = cursor.scalar() or False
                event.set(exists=result)
                return result

    async def count(
        self,
        table_name: str,
        /,
        distinct: SelectorTypes = False,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> int:
        with self._audit("count") as event:
            table = self.get_table(table_name)
            selectors = Selectors.resolve(table, distinct)
            condition = Condition.create(table, where, **query)
            statement = table.count(selectors, condition)
            async with self.execute(statement) as cursor:
                result = cursor.scalar() or 0
                event.set(count=result)
                return result

    async def insert(
        self,
        table_name: str,
        *rows: Row,
        on_conflict: Iterable[str] | None = None,
        update: SelectorTypes = None,
        return_pks: bool = True,
    ) -> list[int]:
        with self._audit("insert") as event:
            rows_ = [self.serialize(row) for row in rows]
            table = self.get_table(table_name)
            on_conflict_ = list(on_conflict) if on_conflict else []
            update_ = Selectors.resolve(table, update, only_columns=True)
            pks: list[int] = []
            # MySQL doesn't support RETURNING, so if PKs are required we have to insert rows one by one.
            try:
                if return_pks and self.is_mysql:
                    for row in rows_:
                        row[table.pk.name] = None
                    statement = table.insert([], on_conflict_, update_)
                    for row in rows_:
                        # Make sure the PK is present in the INSERT statement (for ON CONFLICT DO NOTHING to work).
                        row[table.pk.name] = None
                        async with self.execute(statement.values(row), autocommit=True) as cursor:
                            pk = cursor.inserted_primary_key[0]
                            if pk:  # might be 0 (ON CONFLICT DO NOTHING).
                                pks.append(pk)
                else:
                    statement = table.insert(rows_, on_conflict_, update_, return_pks=return_pks)
                    async with self.execute(statement, autocommit=True) as cursor:
                        if return_pks:
                            pks.extend(getattr(row, table.pk.name) for row in cursor)
            except IntegrityError as error:
                raise self._normalize_integrity_error(error, table, rows_)
            event.set(pks=pks or None)
            return pks

    def update(
        self,
        table_name: str,
        hook: Callable[[dict[str, Any]], AsyncContextManager[None]] | None = None,
        /,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> Callable[..., Awaitable[int]]:
        with self._audit("update"):
            table = self.get_table(table_name)
            condition = Condition.create(table, where, **query)
            statement = table.update(condition)

            async def set(**values: Any) -> int:
                with self._audit("updating") as event:
                    if hook:
                        async with self.transaction():
                            async with hook(values):
                                result = await self._update(table, statement, values)
                    else:
                        result = await self._update(table, statement, values)
                    event.set(updated=result)
                    return result

            return set

    async def delete(self, table_name: str, /, *, where: Expression | Query | None = None, **query: Any) -> int:
        with self._audit("delete") as event:
            table = self.get_table(table_name)
            condition = Condition.create(table, where, **query)
            statement = table.delete(condition)
            async with self.execute(statement, autocommit=True) as cursor:
                result = cursor.rowcount
                event.set(deleted=result)
                return result

    async def select(
        self,
        table_name: str,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order: str | Iterable[str] | None = None,
        **query: Any,
    ) -> list[Row]:
        with self._audit("select") as event:
            table = self.get_table(table_name)
            selectors = Selectors.resolve(table, fields)
            condition = Condition.create(table, where, **query)
            order_ = Selectors.resolve(table, order)
            statement = table.select(selectors, condition, limit=limit, offset=offset, order=order_)
            results: list[Row] = []
            event.set(rows=results)
            async with self.execute(statement, autocommit=False) as cursor:
                return [self.deserialize(row._asdict()) for row in cursor]

    async def select_one(
        self,
        table_name: str,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> Row:
        with self._audit("select_one") as event:
            table = self.get_table(table_name)
            selectors = Selectors.resolve(table, fields)
            condition = Condition.create(table, where, **query)
            statement = table.select(selectors, condition, limit=1)
            async with self.execute(statement, autocommit=False) as cursor:
                row = cursor.first()
                if row is None:
                    if condition:
                        message = f"{table.name} with {condition} doesn't exist"
                    else:
                        message = f"no {table.plural} exist"
                    raise DoesNotExistError(message)
                result = self.deserialize(row._asdict())
                event.set(row=result)
                return result

    async def link(
        self,
        table_name: str,
        m2m_name: str,
        sources: Iterable[int],
        targets: Iterable[int],
    ) -> int:
        with self._audit("link") as event:
            table = self.get_table(table_name)
            statement = table.link(m2m_name, sources, targets)
            async with self.execute(statement, autocommit=True) as cursor:
                result = cursor.rowcount
                event.set(linked=result)
                return result

    async def unlink(
        self,
        table_name: str,
        m2m_name: str,
        sources: Iterable[int],
        targets: Iterable[int],
    ) -> int:
        with self._audit("unlink") as event:
            table = self.get_table(table_name)
            statement = table.unlink(m2m_name, sources, targets)
            async with self.execute(statement, autocommit=True) as cursor:
                result = cursor.rowcount
                event.set(unlinked=result)
                return result

    @asynccontextmanager
    async def _transaction(self, transaction: AsyncTransaction) -> AsyncIterator[AsyncTransaction]:
        # We store the transaction for nested calls.
        token = self.active_transaction.set(transaction)
        try:
            yield transaction
        except Exception:
            # The transaction might have been invaliated in a nested context, so we need to check if it's active first.
            if transaction.is_active:
                with self._audit("rollback"):
                    await transaction.rollback()
            raise
        else:
            # The transaction might have been invaliated in a nested context, so we need to check if it's active first.
            if transaction.is_active:
                with self._audit("commit"):
                    await transaction.commit()
        finally:
            self.active_transaction.reset(token)

    def _url_with_driver(self, url: str) -> str:
        scheme, rest = url.split("://", 1)
        if scheme.startswith("sqlite"):
            return url if "+" in scheme else f"{scheme}+aiosqlite://{rest}"
        if scheme.startswith("postgresql"):
            return url if "+" in scheme else f"{scheme}+asyncpg://{rest}"
        if scheme.startswith("mysql"):
            return url if "+" in scheme else f"{scheme}+aiomysql://{rest}"
        scheme = scheme.split("+")[0]
        raise RuntimeError(f"dialect {scheme!r} is not supported (available dialects are sqlite, postgresql and mysql)")

    def _configure_sqlite(self, connection: sqlite3.Connection, _: Any) -> None:
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    def _audit(self, event: str, /, **data: Any) -> AuditEventBase:
        if self.auditor is None:
            return AuditEventBase()
        return AuditEvent(self, event, data)  # type: ignore

    def _get_relevant_tables(self, table_names: Iterable[str] | None = None) -> list[Table]:
        if not table_names:
            table_names = tuple(self._tables)
        tables: set[Table] = set()
        for name in table_names:
            tables.add(self.get_table(name))
            for _, link_table_name in self._m2ms[name].values():
                tables.add(self.get_table(link_table_name))
        return list(tables)

    def _format_clause(self, clause: ClauseElement, values: Mapping[str, Any] | None = None) -> str:
        compiled = cast(SQLCompiler, clause.compile(dialect=self.engine.dialect))
        parameters = [compiled.binds[name].value for name in compiled.positiontup or []]
        if values:
            parameters.extend(values.values())
        iterator = iter(parameters)

        def replace(match: re.Match[str]) -> str:
            parameter = next(iterator)
            if isinstance(parameter, list | dict):
                return repr(json.dumps(parameter))
            return repr(parameter)

        if self.is_sqlite:
            pattern = SQLITE_PARAMETER
        elif self.is_postgresql:
            pattern = POSTGRESQL_PARAMETER
        else:  # MySQL
            pattern = MYSQL_PARAMETER
        return pattern.sub(replace, compiled.string)

    async def _update(self, table: Table, statement: Update, values: dict[str, Any]) -> int:
        for key, value in values.items():
            if isinstance(value, Expression):
                values[key], _ = value.resolve(table)
        values = self.serialize(values)
        async with self.execute(statement.values(values), autocommit=True) as cursor:
            return cursor.rowcount

    def _normalize_integrity_error(self, error: IntegrityError, table: Table, rows: list[Row]) -> Exception:
        conflicts: dict[str, set[Any]] = {}
        with suppress(Exception):
            if self.is_sqlite and (match := SQLITE_UNIQUE_ERROR.search(str(error))):
                for field in match.group(1).split(","):
                    field = field.strip().removeprefix(f"{table.name}.")
                    conflicts[field] = {str(row[field]) for row in rows}
            if self.is_postgresql and (match := POSTGRESQL_UNIQUE_ERROR.search(str(error))):
                fields, values = match.groups()
                for field, value in zip(fields.split(","), values.split(",")):
                    conflicts[field.strip()] = {value.strip()}
            if self.is_mysql and (match := MYSQL_UNIQUE_ERROR.search(str(error))):
                values, field = match.groups()
                field = field.removeprefix(f"{table.name}.")
                for constraint in table.unique:
                    if field in constraint:
                        for field, value in zip(constraint, values.split("-")):
                            conflicts[field] = {value}
                        break
                else:
                    conflicts[field] = {values}
        if not conflicts:
            return error
        conflict: list[str] = []
        for field, values in conflicts.items():
            if len(values) == 1:
                [value] = values
                conflict.append(f"{field} {value!r}")
            else:
                conflict.append(f"{field} in {values}")
        return AlreadyExistsError(f"{table.name} with {and_(conflict)} already exists")
