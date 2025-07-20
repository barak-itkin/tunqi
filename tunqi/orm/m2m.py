from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterable, Self, overload

from tunqi.core.expression import Expression
from tunqi.core.query import Query
from tunqi.core.selector import SelectorTypes
from tunqi.core.table import Table

if TYPE_CHECKING:
    from tunqi.orm.model import Model


class M2M[T: Model]:

    def __init__(self, name: str, source_model: type[Model], model: type[T]) -> None:
        self.name = name
        self.source_model = source_model
        self.model = model

    def __str__(self) -> str:
        return f"many-to-many relation {self.source_model.config.name}.{self.name} <-> {self.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @overload
    def __get__[S: Model](self, model: None, model_class: type[S]) -> Self: ...

    @overload
    def __get__[S: Model](self, model: S, model_class: type[S]) -> BoundM2M[S, T]: ...

    def __get__[S: Model](self, model: S | None, model_class: type[S]) -> Self | BoundM2M[S, T]:
        if model is None:
            return self
        return BoundM2M(self, model)

    def __set__(self, model: Model, value: Any) -> None:
        raise ValueError(f"{self} can't be set directly")

    @cached_property
    def to(self) -> str:
        for m2m in self.model.config.m2ms.values():
            if m2m.model is self.source_model:
                return m2m.name
        return self.source_model.config.plural


class BoundM2M[S: Model, T: Model]:

    def __init__(self, m2m: M2M[T], source: S) -> None:
        self.m2m = m2m
        self.source = source

    def __str__(self) -> str:
        pk = self.source.pk or "?"
        source = f"{self.m2m.source_model.config.name}[{pk}].{self.m2m.name}"
        return f"many-to-many relation {source} <-> {self.m2m.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @property
    def model(self) -> type[T]:
        return self.m2m.model

    async def exists(self, /, *, where: Expression | Query | None = None, **query: Any) -> bool:
        query[self._link] = self._assert_saved()
        return await self.model.exists(where=where, **query)

    async def count(
        self,
        /,
        distinct: SelectorTypes = False,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> int:
        query[self._link] = self._assert_saved()
        return await self.model.count(distinct=distinct, where=where, **query)

    async def get(self, /, *, where: Expression | Query | None = None, **query: Any) -> T:
        query[self._link] = self._assert_saved()
        return await self.model.get(where=where, **query)

    async def get_fields(
        self,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> dict[str, Any]:
        query[self._link] = self._assert_saved()
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
        query[self._link] = self._assert_saved()
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
        query[self._link] = self._assert_saved()
        return await self.model.all_fields(fields, where=where, limit=limit, offset=offset, order=order, **query)

    async def add(self, *models: T) -> int:
        source_pk = self._assert_saved()
        target_pks = self._assert_models(models)
        return await self.source.config.database.link(
            self.model.config.table_name, self.m2m.to, target_pks, [source_pk]
        )

    async def remove(self, *models: T) -> int:
        source_pk = self._assert_saved()
        target_pks = self._assert_models(models)
        return await self.source.config.database.unlink(
            self.model.config.table_name, self.m2m.to, target_pks, [source_pk]
        )

    @property
    def _link(self) -> str:
        return f"{self.m2m.to}__{Table.pk_name}"

    def _assert_saved(self) -> int:
        if not self.source.pk:
            raise RuntimeError(f"can't operate on a {self} of the unsaved {self.source}")
        return self.source.pk

    def _assert_models(self, models: Iterable[T]) -> list[int]:
        pks: list[int] = []
        for n, model in enumerate(models, 1):
            if model.pk is None:
                raise ValueError(f"can't use {model!r} (item #{n}) with a {self} (expected {self.model.config.name})")
            pks.append(model.pk)
        return pks
