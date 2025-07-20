from __future__ import annotations

import datetime as dt
import time
from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Mapping, Self, cast

import sqlparse
from sqlalchemy import ClauseElement, Executable

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.database import Database

type Auditor = Callable[[AuditEvent], None]


class AuditEventBase:

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        pass

    def set(self, **_) -> None:
        pass

    def set_statement(self, *_) -> None:
        pass


class AuditEvent(AuditEventBase):

    _active_event: ClassVar[ContextVar[AuditEvent | None]] = ContextVar("active_event", default=None)
    __slots__ = "database", "name", "data", "start_time", "end_time", "error", "children", "_parent"

    def __init__(
        self,
        database: Database,
        name: str,
        data: dict[str, Any],
    ) -> None:
        self.database = database
        self.name = name
        self.data = data
        self.start_time = time.time()
        self.end_time: float | None = None
        self.error: Exception | None = None
        self.children: list[AuditEvent] = []
        self._parent: AuditEvent | None = None

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<audit event {self.name!r}>"

    def __enter__(self) -> Self:
        self._parent = self._active_event.get()
        self._active_event.set(self)
        if self._parent:
            self._parent.children.append(self)
        return self

    def __exit__(self, exception: type[Exception] | None, error: Exception | None, tb: TracebackType | None) -> None:
        self.end_time = time.time()
        self.error = error
        self._active_event.set(self._parent)
        if not self._parent and self.database.auditor:
            self.database.auditor(self)
        else:
            self._parent = None

    @property
    def duration(self) -> float:
        if not self.start_time or not self.end_time:
            return 0.0
        return self.end_time - self.start_time

    @property
    def start_datetime(self) -> dt.datetime | None:
        if not self.start_time:
            return None
        return dt.datetime.fromtimestamp(self.start_time, tz=dt.UTC).astimezone()

    @property
    def end_datetime(self) -> dt.datetime | None:
        if not self.end_time:
            return None
        return dt.datetime.fromtimestamp(self.end_time, tz=dt.UTC).astimezone()

    def set(self, **data: Any) -> None:
        self.data.update(data)

    def set_statement(self, statement: Executable, values: Mapping[str, Any] | None = None) -> None:
        clause = cast(ClauseElement, statement)
        output = self.database._format_clause(clause, values)
        output = sqlparse.format(output, reindent=True, keyword_case="upper")
        self.set(statement=output)
