from __future__ import annotations

import inspect
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, get_type_hints
from weakref import WeakValueDictionary

from pydantic._internal._model_construction import ModelMetaclass as BaseModelMetaclass

from tunqi.core.table import Table
from tunqi.orm.annotations import (
    annotation_schema,
    is_classvar,
    parse_relation,
    parse_relations,
)
from tunqi.sync.backref import Backref
from tunqi.sync.database import Database
from tunqi.sync.fk import FK, OptionalFK
from tunqi.sync.m2m import M2M
from tunqi.utils import pluralize, to_snake_case

if TYPE_CHECKING:
    from .model import Model

UNIQUE_TOGETHER = "unique_together"


class ModelType(BaseModelMetaclass):

    base: ClassVar[type[Model]]

    @classmethod
    def __prepare__(
        mcs,
        /,
        *args: Any,
        table_name: str | None = None,
        plural: str | None = None,
        abstract: bool = False,
        deduplicate: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        namespace = super().__prepare__(*args, **kwargs)
        namespace[UNIQUE_TOGETHER] = set()
        return namespace

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        attributes: dict[str, Any],
        /,
        *,
        table_name: str | None = None,
        plural: str | None = None,
        abstract: bool = False,
        deduplicate: bool | None = None,
        **kwargs: Any,
    ) -> type:
        unique: set[tuple[str, ...]] = attributes.pop(UNIQUE_TOGETHER)
        # Pydantic doesn't support annotated descriptors, so we have to extract the relations before creating the class.
        relations = ModelConfig.extract_relations(attributes)
        model_class: type[Model] = super().__new__(mcs, name, bases, attributes, **kwargs)
        if not hasattr(mcs, "base"):
            mcs.base = model_class  # type: ignore
        config = ModelConfig(model_class, relations, table_name, plural, unique, deduplicate, abstract)
        config._bind()
        return model_class


class ModelConfig[T: Model]:

    def __init__(
        self,
        model_class: type[T],
        relations: dict[str, Any],
        table_name: str | None,
        plural: str | None,
        unique: set[tuple[str, ...]],
        deduplicate: bool | None,
        abstract: bool,
    ) -> None:
        self.model_class = model_class
        self.name = self.model_class.__name__
        self.unique = unique
        self.abstract = abstract
        self.table_name = table_name or to_snake_case(self.name)
        self.plural = plural or pluralize(self.table_name)
        self.classes: dict[str, type[Model]] = {}
        self.instances: WeakValueDictionary[int, T] = WeakValueDictionary()
        self.fks: dict[str, FK[Model]] = {}
        self.backrefs: dict[str, Backref[Model]] = {}
        self.m2ms: dict[str, M2M[Model]] = {}
        self.defined = False
        self._relations = relations
        self._deduplicate = deduplicate
        self._database: Database | None = None

    @property
    def database(self) -> Database:
        if self._database is not None:
            return self._database
        return Database.get()

    @cached_property
    def annotations(self) -> dict[str, Any]:
        # We want to resolve the annotated descriptors as well, but we can't keep them as class annotations for good,
        # lest Pydantic notices them via inheritance; so, we add them before collecting the annotations and remove them
        # right after.
        self.model_class.__annotations__.update(self._relations)
        annotations: dict[str, Any] = {}
        for name, annotation in get_type_hints(self.model_class, include_extras=True).items():
            if name.startswith("_") or is_classvar(annotation):
                continue
            annotations[name] = annotation
        for name in self._relations:
            del self.model_class.__annotations__[name]
        return annotations

    @cached_property
    def schema(self) -> dict[str, Any]:
        columns: dict[str, dict[str, Any]] = {}
        for base in self.model_class.__bases__:
            if not issubclass(base, ModelType.base):
                continue
            columns.update(base.config.schema["columns"])
            self.unique.update(base.config.unique)
        for name, annotation in self.annotations.items():
            if name == Table.pk_name:
                continue
            if name in self._relations:
                columns[name] = self._add_relation(name, annotation)
            else:
                columns[name] = annotation_schema(name, annotation)
        return {
            "columns": columns,
            "plural": self.plural,
            "unique": list(self.unique),
        }

    @cached_property
    def unique_columns(self) -> set[str]:
        return {name for name, column in self.schema["columns"].items() if column.get("unique")}

    @classmethod
    def extract_relations(self, attributes: dict[str, Any]) -> dict[str, Any]:
        annotations = attributes.setdefault("__annotations__", {})
        relations = parse_relations(annotations)
        for name in relations:
            del annotations[name]
        return relations

    def add_tables(self) -> list[str]:
        db = self.database
        tables_names: list[str] = []
        for model_class in self.classes.values():
            db.add_table(model_class.config.table_name, model_class.config.schema)
            tables_names.append(model_class.config.table_name)
        return tables_names

    def define(self) -> None:
        if self.abstract:
            raise ValueError(f"{self.name} is abstract")
        if self.defined:
            return
        self.database.add_table(self.table_name, self.schema)
        self.defined = True
        for fk in self.fks.values():
            fk.model.config.define()
        for backref in self.backrefs.values():
            backref.model.config.define()
        for m2m in self.m2ms.values():
            m2m.model.config.define()

    def set_database(self, database: Database | None) -> None:
        self._database = database
        for cls in self.classes.values():
            cls.config.set_database(database)

    def set_deduplication(self, deduplicate: bool) -> None:
        self._deduplicate = deduplicate
        for cls in self.classes.values():
            cls.config.set_deduplication(deduplicate)

    def deduplicate(self, model: T) -> T:
        if not self._deduplicate or not model.pk:
            return model
        if model.pk not in self.instances:
            self.instances[model.pk] = model
        return self.instances[model.pk]

    def _bind(self) -> None:
        self.model_class.config = self
        if self.name in ModelType.base.config.classes:
            raise ValueError(f"model {self.name!r} already exists")
        # Now that we have the model class, we can also merge in any inherited relations.
        relations: dict[str, Any] = {}
        for baseclass in reversed(self.model_class.__mro__):
            if not issubclass(baseclass, ModelType.base):
                continue
            relations.update(baseclass.config._relations)
        self._relations = relations
        # At this point, some of the annotations will probably be forward references, because relations are inherently
        # circular:
        # class A(Model):
        #     b: Backref[B]  # <- forward reference
        # class B(Model):
        #     a: FK[A]
        # As such, we can't use get_type_hints() yet, and would have to parse annotation strings manually, which sucks.
        # So instead, we replace the actual relations with pass-through descriptors that evaluate and replace themselves
        # with their real counterparts lazily, once all the classes are defined.
        for name in self._relations:
            setattr(self.model_class, name, Relation(name))
        # If the class is abstract, there's nothing else to do.
        if self.abstract:
            return
        # Otherwise, we must notify its baseclasses of its existence.
        for baseclass in reversed(self.model_class.__mro__):
            if not issubclass(baseclass, ModelType.base):
                continue
            baseclass.config.classes[self.name] = self.model_class

    def _add_relation(self, name: str, annotation: Any) -> dict[str, Any]:
        relation = parse_relation(name, annotation)
        if not relation:
            raise ValueError(f"invalid relation annotation for {name!r}: {annotation!r}")
        relation_type, target_name, qualifiers = relation
        nullable = qualifiers.get("nullable", False)
        unique = qualifiers.get("unique", False)
        index = qualifiers.get("index", False)
        target = ModelType.base.get_model(target_name)
        table_name = target.config.table_name
        if nullable:
            raise ValueError(f"{relation_type} {name!r} cannot be nullable")
        if relation_type == FK.__name__:
            self.fks[name] = FK(name, self.model_class, target)
            return {"type": "fk", "table": table_name, "nullable": False, "unique": unique, "index": index}
        if relation_type == OptionalFK.__name__:
            self.fks[name] = OptionalFK(name, self.model_class, target)
            return {"type": "fk", "table": table_name, "nullable": True, "unique": unique, "index": index}
        if unique:
            raise ValueError(f"{relation_type} {name!r} cannot be unique")
        if index:
            raise ValueError(f"{relation_type} {name!r} cannot be indexed")
        if relation_type == Backref.__name__:
            self.backrefs[name] = Backref(name, self.model_class, target)
            return {"type": "backref", "table": table_name}
        self.m2ms[name] = M2M(name, self.model_class, target)
        return {"type": "m2m", "table": table_name}


class Relation:

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, model: Model | None, model_class: type[Model]) -> Any:
        relation = self._get_relation(model_class, self.name)
        setattr(model_class, self.name, relation)
        bound_relation = relation.__get__(model, model_class)
        return bound_relation

    def __set__(self, model: Model, value: Any) -> None:
        raise ValueError(f"can't set {self.name} directly")

    def _get_relation(self, model_class: type[Model], name: str) -> FK | Backref | M2M:
        if name in model_class.config.fks:
            return model_class.config.fks[name]
        elif name in model_class.config.backrefs:
            return model_class.config.backrefs[name]
        else:
            return model_class.config.m2ms[name]


def unique(*fields: str) -> None:
    frame = inspect.currentframe()
    frame = frame and frame.f_back
    if not frame:
        raise RuntimeError("unable to get current frame")
    unique: set[tuple[str, ...]] = frame.f_locals.get(UNIQUE_TOGETHER, set())
    unique.add(fields)
