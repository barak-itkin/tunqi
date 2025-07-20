import re

import pytest

from tunqi.sync import Model

from .conftest import T


def test_delete_one(t1: T, t2: T) -> None:
    T.create(t1, t2)
    assert T.count() == 2
    t1.delete()
    assert T.count() == 1
    t2.delete()
    assert T.count() == 0


def test_delete_before_and_after() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        def before_delete(self):
            deleted.append(self.pk)

        def after_delete(self):
            deleted.append(self.pk)

    Model.create_tables()
    a = A(n=1)
    a.save()
    a.delete()
    assert deleted == [1, None]


def test_delete_state_restoration() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        def after_delete(self):
            deleted.append(self.pk)
            raise ValueError()

    Model.create_tables()
    a = A(n=1)
    a.save()
    with pytest.raises(ValueError):
        a.delete()
    assert a.pk == 1
    assert A.count() == 1
    assert deleted == [None]


def test_delete_many(t1: T, t2: T) -> None:
    t1.n = 1
    T.create(t1, t2)
    assert T.count() == 2
    assert T.delete_all(n=1) == 2
    assert T.count() == 0
    assert T.delete_all(n=1) == 0


def test_delete_many_before_and_after() -> None:
    deleted: list[int | None] = []

    class A(Model):
        n: int

        def before_delete(self):
            deleted.append(self.pk)

        def after_delete(self):
            deleted.append(self.pk)

    Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    A.create(a1, a2)
    A.delete_all(a1, a2)
    assert deleted == [1, 2, None, None]


def delete_many_invalid_model(t1: T) -> None:
    class A(Model):
        n: int

    a = A(n=1)
    with pytest.raises(ValueError, match=re.escape(f"{a} (item #2) is a a, not a t")):
        T.delete_all(t1, a)
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) is a t, not a a")):
        A.delete_all(t1, a)


def test_delete_many_nonexisting_model(t1: T, t2: T) -> None:
    t2.save()
    with pytest.raises(ValueError, match=re.escape(f"{t1} (item #1) doesn't exist")):
        T.delete_all(t1, t2)
    t2.delete()
    t1.save()
    with pytest.raises(ValueError, match=re.escape(f"{t2} (item #2) doesn't exist")):
        T.delete_all(t1, t2)


def test_delete_many_transaction() -> None:
    class A(Model):
        n: int

        def after_delete(self):
            if self.n > 1:
                raise ValueError("n > 1")

    Model.create_tables()
    a1 = A(n=1)
    a2 = A(n=2)
    A.create(a1, a2)
    with pytest.raises(ValueError, match="n > 1"):
        A.delete_all(a1, a2)
    assert a1.pk is not None
    assert a2.pk is not None
    assert A.count() == 2
