from __future__ import annotations

from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Self,
    overload,
)

from tunqi.core.expression import Expression
from tunqi.core.query import Query
from tunqi.core.selector import SelectorTypes

if TYPE_CHECKING:
    from tunqi.orm.fk import BoundFK
    from tunqi.orm.model import Model


class Backref[T: Model]:

    def __init__(self, name: str, source_model: type[Model], model: type[T]) -> None:
        self.name = name
        self.source_model = source_model
        self.model = model

    def __str__(self) -> str:
        return f"backreference {self.source_model.config.name}.{self.name} -> {self.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @overload
    def __get__[S: Model](self, model: None, model_class: type[S]) -> Self: ...

    @overload
    def __get__[S: Model](self, model: S, model_class: type[S]) -> BoundBackref[S, T]: ...

    def __get__[S: Model](self, model: S | None, model_class: type[S]) -> Self | BoundBackref[S, T]:
        if model is None:
            return self
        return BoundBackref(self, model)

    def __set__(self, model: Model, value: Any) -> None:
        raise ValueError(f"{self} can't be set directly")

    @cached_property
    def to(self) -> str:
        for fk in self.model.config.fks.values():
            if fk.model is self.source_model:
                return fk.name
        raise ValueError(f"can't find foreign key to {self.source_model.config.name} in {self.model.config.name}")


class BoundBackref[S: Model, T: Model]:

    def __init__(self, backref: Backref[T], source: S) -> None:
        self.backref = backref
        self.source = source

    def __str__(self) -> str:
        pk = self.source.pk or "?"
        source = f"{self.backref.source_model.config.name}[{pk}].{self.backref.name}"
        return f"backreference {source} -> {self.backref.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @property
    def model(self) -> type[T]:
        return self.backref.model

    async def exists(self, /, *, where: Expression | Query | None = None, **query: Any) -> bool:
        query[self.backref.to] = self._assert_saved()
        return await self.model.exists(where=where, **query)

    async def count(
        self,
        /,
        distinct: SelectorTypes = False,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> int:
        query[self.backref.to] = self._assert_saved()
        return await self.model.count(distinct=distinct, where=where, **query)

    async def get(self, /, *, where: Expression | Query | None = None, **query: Any) -> T:
        query[self.backref.to] = self._assert_saved()
        return await self.model.get(where=where, **query)

    async def get_fields(
        self,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> dict[str, Any]:
        query[self.backref.to] = self._assert_saved()
        return await self.model.get_fields(fields=fields, where=where, **query)

    async def all(
        self,
        /,
        where: Expression | Query | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order: Iterable[str] | None = None,
        **query: Any,
    ) -> list[T]:
        query[self.backref.to] = self._assert_saved()
        return await self.model.all(where=where, limit=limit, offset=offset, order=order, **query)

    async def all_fields(
        self,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order: Iterable[str] | None = None,
        **query: Any,
    ) -> list[dict[str, Any]]:
        query[self.backref.to] = self._assert_saved()
        return await self.model.all_fields(fields, where=where, limit=limit, offset=offset, order=order, **query)

    async def create(
        self,
        *models: T,
        on_conflict: Iterable[str] | None = None,
        update: SelectorTypes = None,
    ) -> list[int]:
        pk = self._assert_saved()
        fks: list[BoundFK[S, T]] = []
        fk_pks: list[int | None] = []
        for model in models:
            fk: BoundFK[S, T] = getattr(model, self.backref.to)
            fks.append(fk)
            fk_pks.append(fk.pk)
            fk.pk = pk
        try:
            return await self.model.create(*models, on_conflict=on_conflict, update=update)
        except Exception:
            for fk, fk_pk in zip(fks, fk_pks):
                fk.pk = fk_pk
            raise

    def update(
        self,
        /,
        *targets: int | T | None,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> Callable[..., Awaitable[int]]:
        query[self.backref.to] = self._assert_saved()
        return self.model.update(*targets, where=where, **query)

    async def delete(self, *targets: int | T | None, where: Expression | Query | None = None, **query: Any) -> int:
        query[self.backref.to] = self._assert_saved()
        return await self.model.delete_all(*targets, where=where, **query)

    def _assert_saved(self) -> int:
        if not self.source.pk:
            raise RuntimeError(f"can't operate on a {self} of the unsaved {self.source}")
        return self.source.pk
