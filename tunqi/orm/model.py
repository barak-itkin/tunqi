from __future__ import annotations

import pathlib
from contextlib import asynccontextmanager
from functools import reduce
from operator import or_
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    Literal,
    Mapping,
    Self,
    overload,
    override,
)

from pydantic import BaseModel
from sqlalchemy import CursorResult, Executable

from tunqi.core.database import Database
from tunqi.core.expression import Expression
from tunqi.core.query import Query, q
from tunqi.core.selector import SelectorTypes
from tunqi.core.table import Table
from tunqi.orm.annotations import PK
from tunqi.orm.fk import FK, BoundFK
from tunqi.orm.model_type import ModelConfig, ModelType
from tunqi.utils import and_


class Model(BaseModel, metaclass=ModelType, abstract=True):

    config: ClassVar[ModelConfig]

    pk: PK | None = None
    _state: dict[str, Any]

    def __init__(self, *args, **data) -> None:
        self._assign_positional_args(args, data)
        fks = self._extract_fks(data)
        super().__init__(**data)
        self._assign_fks(fks)
        self._state = {}

    def __str__(self) -> str:
        attributes = [f"{self.pk or "?"}"]
        for key, value in self.model_dump().items():
            if key == Table.pk_name:
                continue
            attributes.append(f"{key}={value!r}")
        return f'{type(self).__name__}({", ".join(attributes)})'

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.model_dump() == other.model_dump()

    @classmethod
    def get_model(cls, name: str) -> type[Model]:
        if name not in Model.config.classes:
            raise ValueError(f"model {name!r} does not exist (available models are {and_(Model.config.classes)})")
        return Model.config.classes[name]

    @classmethod
    def use(cls, database: str | Database | None) -> None:
        if isinstance(database, str):
            database = Database(database)
        cls.config.set_database(database)

    @classmethod
    async def create_tables(cls) -> None:
        table_names = cls.config.add_tables()
        await cls.config.database.create_tables(table_names)

    @classmethod
    async def drop_tables(cls) -> None:
        table_names = cls.config.add_tables()
        await cls.config.database.drop_tables(table_names)

    @classmethod
    async def make_migrations(cls, migrations_directory: str | pathlib.Path) -> None:
        table_names = cls.config.add_tables()
        await cls.config.database.make_migrations(migrations_directory, table_names)

    @classmethod
    async def migrate(cls, migrations_directory: str | pathlib.Path) -> None:
        cls.config.add_tables()
        await cls.config.database.migrate(migrations_directory)

    @classmethod
    def get_table(cls) -> Table:
        cls.config.define()
        return cls.config.database.get_table(cls.config.table_name)

    @classmethod
    async def execute(
        cls,
        statement: str | Executable,
        values: Mapping[str, Any] | None = None,
        *,
        autocommit: bool = False,
    ) -> AsyncIterator[CursorResult]:
        async with cls.config.database.execute(statement, values, autocommit=autocommit) as cursor:
            yield cursor

    @classmethod
    async def exists(cls, pk: int | None = None, /, *, where: Expression | Query | None = None, **query: Any) -> bool:
        cls.config.define()
        if pk is not None:
            query[Table.pk_name] = pk
        query.update(cls.model_query())
        return await cls.config.database.exists(cls.config.table_name, where=where, **query)

    @classmethod
    async def count(
        cls,
        /,
        distinct: SelectorTypes = False,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> int:
        cls.config.define()
        query.update(cls.model_query())
        return await cls.config.database.count(cls.config.table_name, distinct=distinct, where=where, **query)

    @classmethod
    async def create(
        cls,
        *models: Model,
        on_conflict: Iterable[str] | None = None,
        update: SelectorTypes = None,
    ) -> list[int]:
        cls.config.define()
        for n, model in enumerate(models, 1):
            cls._assert_model(n, model, exists=False)
        return await cls._create(*models, on_conflict=on_conflict, update=update)

    @classmethod
    def update(
        cls,
        /,
        *targets: Model | int | None,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> Callable[..., Awaitable[int]]:
        cls.config.define()
        return cls._update(*targets, where=where, **query)

    @classmethod
    async def delete_all(
        cls,
        /,
        *targets: Model | int | None,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> int:
        cls.config.define()
        return await cls._delete(*targets, where=where, **query)

    @classmethod
    async def get(cls, pk: int | None = None, /, *, where: Expression | Query | None = None, **query: Any) -> Self:
        cls.config.define()
        if pk is not None:
            query[Table.pk_name] = pk
        query.update(cls.model_query())
        model_dict = await cls.config.database.select_one(cls.config.table_name, where=where, **query)
        model = cls(**model_dict)
        model._set_state(model_dict)
        model = cls.config.deduplicate(model)
        return model

    @classmethod
    async def get_or_create(cls, /, **attributes: Any) -> Self:
        cls.config.define()
        unique: set[str] = set()
        for column in cls.config.unique_columns:
            if column in attributes:
                unique.add(column)
        for constraint in cls.config.unique:
            if all(column in attributes for column in constraint):
                unique.update(*constraint)
        model = cls(**attributes)
        [model.pk] = await cls.create(model, on_conflict=unique, update=False)
        return model

    @classmethod
    async def get_fields(
        cls,
        pk: int | None = None,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> dict[str, Any]:
        cls.config.define()
        if pk is not None:
            query[Table.pk_name] = pk
        query.update(cls.model_query())
        return await cls.config.database.select_one(cls.config.table_name, fields=fields, where=where, **query)

    @classmethod
    async def all(
        cls,
        /,
        *,
        where: Expression | Query | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order: Iterable[str] | None = None,
        **query: Any,
    ) -> list[Self]:
        cls.config.define()
        query.update(cls.model_query())
        model_dicts = await cls.config.database.select(
            cls.config.table_name,
            where=where,
            limit=limit,
            offset=offset,
            order=order,
            **query,
        )
        models: list[Self] = []
        for model_dict in model_dicts:
            model = cls(**model_dict)
            model._set_state(model_dict)
            model = cls.config.deduplicate(model)
            models.append(model)
        return models

    @classmethod
    async def all_fields(
        cls,
        /,
        fields: SelectorTypes = True,
        *,
        where: Expression | Query | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order: Iterable[str] | None = None,
        **query: Any,
    ) -> list[dict[str, Any]]:
        cls.config.define()
        query.update(cls.model_query())
        return await cls.config.database.select(
            cls.config.table_name,
            fields=fields,
            where=where,
            limit=limit,
            offset=offset,
            order=order,
            **query,
        )

    @classmethod
    async def refresh_all(cls, *targets: Model) -> None:
        cls.config.define()
        pks, models_ = cls._assert_models(targets)
        models = {model.pk: model for model in models_}
        query: dict[str, Any] = {f"{Table.pk_name}__in": pks}
        model_dicts = await cls.all_fields(**query)
        for model_dict in model_dicts:
            model = models[model_dict[Table.pk_name]]
            model._set(model_dict)

    @classmethod
    def model_query(cls) -> dict[str, Any]:
        return {}

    @override
    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        data = super().model_dump(**kwargs)
        for fk_name in self.config.fks:
            fk_value = self.__dict__.get(fk_name)
            if isinstance(fk_value, Model | BoundFK):
                data[fk_name] = fk_value.pk
            else:
                data[fk_name] = fk_value
        return data

    def set(self, **values: Any) -> None:
        fks = self._extract_fks(values)
        for key, value in values.items():
            setattr(self, key, value)
        self._assign_fks(fks)

    def reset(self) -> None:
        self.set(**self._state)

    async def save(self) -> Self:
        self.config.define()
        if self.pk is None:
            await self._create(self)
        else:
            changes = {key: new_value for key, (_, new_value) in self.changed().items()}
            if changes:
                await self._update(self, reset=False)(**changes)
        return self

    async def delete(self) -> Self:
        self.config.define()
        await self._delete(self)
        return self

    async def refresh(self) -> Self:
        self.config.define()
        model_dict = await self.get_fields(self.pk)
        self._set(model_dict)
        return self

    def changed(self) -> dict[str, tuple[Any, Any]]:
        if not self.pk:
            return {}
        values = self._dump_values()
        changed: dict[str, Any] = {}
        for key, new_value in values.items():
            old_value = self._state.get(key)
            if new_value != old_value:
                changed[key] = old_value, new_value
        return changed

    async def before_save(self) -> None:
        pass

    async def after_save(self) -> None:
        pass

    async def before_create(self) -> None:
        pass

    async def after_create(self) -> None:
        pass

    async def before_update(self) -> None:
        pass

    async def after_update(self) -> None:
        pass

    async def before_delete(self) -> None:
        pass

    async def after_delete(self) -> None:
        pass

    @classmethod
    async def _create(
        cls,
        *models: Self,
        on_conflict: Iterable[str] | None = None,
        update: SelectorTypes = None,
    ) -> list[int]:
        states: list[dict[str, Any]] = []
        for model in models:
            await model.before_save()
            await model.before_create()
            states.append(model._dump_values())
        db = cls.config.database
        async with db.transaction():
            try:
                pks = await db.insert(
                    cls.config.table_name,
                    *states,
                    on_conflict=on_conflict,
                    update=update,
                    return_pks=True,
                )
                if len(pks) != len(models) and on_conflict:
                    queries = [q(**{column: state[column] for column in on_conflict}) for state in states]
                    fields = await cls.all_fields(fields="pk", where=reduce(or_, queries))
                    pks = [field["pk"] for field in fields]
                for model, pk, state in zip(models, pks, states):
                    model.pk = pk
                    model._state = state
                    await model.after_create()
                    await model.after_save()
                return pks
            except Exception:
                for model in models:
                    model.pk = None
                    model._state = {}
                raise

    @classmethod
    def _update(
        cls,
        *targets: int | Self | None,
        reset: bool = True,
        where: Expression | Query | None = None,
        **query: Any,
    ) -> Callable[..., Awaitable[int]]:
        pks, models = cls._assert_models(targets)
        if len(pks) == 1:
            query[Table.pk_name] = pks[0]
        elif pks:
            query[f"{Table.pk_name}__in"] = pks

        @asynccontextmanager
        async def hook(values: dict[str, Any]) -> AsyncIterator[None]:
            for model in models:
                await model.before_save()
                await model.before_update()
            yield
            try:
                states: list[dict[str, Any]] = []
                for model in models:
                    model.set(**values)
                    states.append(model._state.copy())
                    model._state.update(values)
                    await model.after_update()
                    await model.after_save()
            except Exception:
                for model, state in zip(models, states):
                    model._state = state
                    if reset:
                        model.reset()
                raise

        return cls.config.database.update(cls.config.table_name, hook, where=where, **query)

    @classmethod
    async def _delete(cls, *targets: int | Self | None, where: Expression | Query | None = None, **query: Any) -> int:
        pks, models = cls._assert_models(targets)
        for model in models:
            await model.before_delete()
        db = cls.config.database
        async with db.transaction():
            try:
                if len(pks) == 1:
                    query[Table.pk_name] = pks[0]
                elif pks:
                    query[f"{Table.pk_name}__in"] = pks
                rowcount = await db.delete(cls.config.table_name, where=where, **query)
                for model in models:
                    model.pk = None
                    await model.after_delete()
                return rowcount
            except Exception:
                for model, pk in zip(models, pks):
                    model.pk = pk
                raise

    @overload
    @classmethod
    def _assert_model(cls, n: int, model: Model, exists: Literal[True]) -> int: ...

    @overload
    @classmethod
    def _assert_model(cls, n: int, model: Model, exists: Literal[False]) -> None: ...

    @classmethod
    def _assert_model(cls, n: int, model: Model, exists: bool) -> int | None:
        count = f" (item #{n})" if n else ""
        if model.config.table_name != cls.config.table_name:
            raise ValueError(f"{model}{count} is a {model.config.table_name}, not a {cls.config.table_name}")
        if exists and model.pk is None:
            raise ValueError(f"{model}{count} doesn't exists")
        if not exists and model.pk is not None:
            raise ValueError(f"{model}{count} already exists")
        return model.pk

    @classmethod
    def _assert_models(cls, targets: tuple[Model | int | None, ...]) -> tuple[list[int], list[Model]]:
        pks: list[int] = []
        models: list[Model] = []
        counter = 1 if len(targets) > 1 else 0
        for n, target in enumerate(targets, counter):
            if target is None:
                continue
            elif isinstance(target, int):
                pks.append(target)
            else:
                pks.append(cls._assert_model(n, target, exists=True))
                models.append(target)
        return pks, models

    def _assign_positional_args(self, args: tuple[Any, ...], data: dict[str, Any]) -> None:
        columns = self.config.schema["columns"]
        if len(args) > len(columns):
            raise ValueError(f"got {len(args)} positional arguments (expected at most {len(columns)})")
        for n, (column, arg) in enumerate(zip(columns, args), 1):
            if column in data:
                raise ValueError(f"redefinition of {column} (both as positional argument #{n} and keyword argument)")
            data[column] = arg

    def _extract_fks(self, data: dict[str, Any]) -> dict[str, Any]:
        return {fk_name: data.pop(fk_name, None) for fk_name in self.config.fks}

    def _assign_fks(self, fks: dict[str, Any]) -> None:
        for fk_name, fk_value in fks.items():
            fk: FK[Model] = self.config.fks[fk_name]
            if fk_value is None or isinstance(fk_value, int):
                self.__dict__[fk_name] = fk_value
                continue
            if isinstance(fk_value, fk.model):
                if not fk_value.pk:
                    raise ValueError(f"can't set {fk} to an unsaved {fk.model.config.name} ({fk_value})")
                self.__dict__[fk_name] = fk_value
                continue
            raise ValueError(f"can't set {fk} to {fk_value!r} (expected PK or {fk.model.config.name})")

    def _dump_values(self) -> dict[str, Any]:
        return self.model_dump(exclude={Table.pk_name})

    def _set_state(self, model_dict: dict[str, Any]) -> None:
        model_dict.pop(Table.pk_name, None)
        self._state = model_dict

    def _set(self, model_dict: dict[str, Any]) -> None:
        self._set_state(model_dict)
        self.set(**self._state)
