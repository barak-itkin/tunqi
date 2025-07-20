from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self, overload

if TYPE_CHECKING:
    from tunqi.orm.model import Model


class FK[T: Model]:

    nullable: ClassVar[bool] = False

    def __init__(self, name: str, source_model: type[Model], model: type[T]) -> None:
        self.name = name
        self.source_model = source_model
        self.model = model

    def __str__(self) -> str:
        return f"foreign key {self.source_model.config.name}.{self.name} -> {self.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @overload
    def __get__[S: Model](self, model: None, model_class: type[S]) -> Self: ...

    @overload
    def __get__[S: Model](self, model: S, model_class: type[S]) -> BoundFK[S, T]: ...

    def __get__[S: Model](self, model: S | None, model_class: type[S]) -> Self | BoundFK[S, T]:
        if model is None:
            return self
        cached = model.__dict__.get(self.name)
        if cached is None or isinstance(cached, int):
            model.__dict__[self.name] = BoundFK(self, model, pk=cached)
        elif not isinstance(cached, BoundFK):
            model.__dict__[self.name] = BoundFK(self, model, object=cached)
        return model.__dict__[self.name]

    def __set__(self, model: Model, value: Any) -> None:
        pass  # pragma: no cover


class BoundFK[S: Model, T: Model]:

    def __init__(self, fk: FK[T], source: S, pk: int | None = None, object: T | None = None) -> None:
        self.fk = fk
        self.source = source
        self._pk = object.pk if object else pk
        self._object = object

    def __str__(self) -> str:
        pk = self.source.pk or "?"
        source = f"{self.fk.source_model.config.name}[{pk}].{self.fk.name}"
        return f"foreign key {source} -> {self.fk.model.config.name}"

    def __repr__(self) -> str:
        return f"<{self}>"

    @property
    def model(self) -> type[T]:
        return self.fk.model

    @property
    def pk(self) -> int | None:
        return self._pk

    @pk.setter
    def pk(self, value: int | None) -> None:
        if value is None and not self.fk.nullable:
            raise ValueError(f"{self} is not nullable")
        self._pk = value
        self._object = None

    @property
    def object(self) -> T | None:
        return self._object

    @object.setter
    def object(self, value: T | None) -> None:
        if value is None and not self.fk.nullable:
            raise ValueError(f"{self} is not nullable")
        self._object = value
        self._pk = value.pk if value else None

    async def get(self, *, fetch: bool = False) -> T | None:
        if self.pk is None:
            return None
        if not self.object or fetch:
            self.object = await self.model.get(self.pk)
        return self.object

    async def set(self, model: int | T | None) -> None:
        object = None
        if model is None:
            if not self.fk.nullable:
                raise ValueError(f"{self} is not nullable")
            pk = None
        elif isinstance(model, int):
            pk = model
        else:
            if type(model) is not self.model:
                raise ValueError(f"can't set {self} to {model!r} (expected {self.model.config.name})")
            elif model.pk is None:
                raise ValueError(f"can't set {self} to the unsaved {model}")
            object, pk = model, model.pk
        if self.source.pk:
            update = {self.fk.name: pk}
            await self.fk.source_model.update(pk=self.source.pk)(**update)
        self.pk = pk
        self.object = object


class OptionalFK[T: Model](FK[T]):
    nullable: ClassVar[bool] = True
