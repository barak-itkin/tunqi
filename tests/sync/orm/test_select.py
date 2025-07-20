import re
from typing import Any

import pytest

from tunqi.sync import DoesNotExistError, Model

from .conftest import T


def test_select_one(t1: T, t2: T) -> None:
    with pytest.raises(DoesNotExistError, match="no ts exist"):
        T.get()
    t1.save()
    t2.save()
    assert T.get(t1.pk) == t1
    assert T.get(t2.pk) == t2
    assert T.get(n=0) == t1
    assert T.get(n=1) == t2
    with pytest.raises(DoesNotExistError, match="t with n == 2 doesn't exist"):
        T.get(n=2)


def test_select(t1: T, t2: T) -> None:
    T.create(t1, t2)
    assert T.all() == [t1, t2]
    T.delete_all()
    assert T.all() == []


def test_select_with_query(t2: T, condition: tuple[str, Any, bool]) -> None:
    key, value, expected = condition
    assert T.all(**{key: value}) == []
    t2.save()
    assert T.all(**{key: value}) == ([t2] if expected else [])


def test_select_with_range(ts: list[T]) -> None:
    T.create(*ts)
    assert T.all(limit=5) == ts[:5]
    assert T.all(limit=5, offset=3) == ts[3:8]
    assert T.all(offset=5) == ts[5:]


def test_select_with_order(ts: list[T]) -> None:
    T.create(*ts)
    assert T.all(order="+n") == ts
    assert T.all(order="-n") == ts[::-1]
    assert T.all(order=["+b", "n"]) == ts[1::2] + ts[::2]
    assert T.all(order=["-b", "n"]) == ts[::2] + ts[1::2]
    assert T.all(order=["b", "-n"]) == ts[-1::-2] + ts[-2::-2]
    assert T.all(order="d.x") == ts
    assert T.all(order="-d.x") == ts[::-1]


def test_select_with_fields(t2: T) -> None:
    assert T.all() == []
    t2.save()
    for key, value in t2.model_dump().items():
        assert T.all_fields(key) == [{key: value}]
        assert T.get_fields(t2.pk, key) == {key: value}
    columns = ["b", "n", "d.x"]
    t2_dict = {"b": t2.b, "n": t2.n, "d.x": t2.d["x"]}
    assert T.all_fields(columns) == [t2_dict]
    assert T.get_fields(t2.pk, columns) == t2_dict


def test_select_with_alias(t2: T) -> None:
    t2.save()
    columns = ["b:B", "n:N", "d.x:X"]
    t2_dict = {"B": t2.b, "N": t2.n, "X": t2.d["x"]}
    assert T.all_fields(columns) == [t2_dict]
    assert T.get_fields(t2.pk, columns) == t2_dict


def test_select_with_function(t2: T) -> None:
    t2.save()
    assert T.all(s__length__gt=2) == [t2]
    assert T.all(s__length__gt=5) == []
    assert T.all(s__length__double=3.0) == [t2]
    assert T.all(s__binary=b"foo") == [t2]
    assert T.all(d__s__length__gt=2) == [t2]
    assert T.all(d__s__length__gt=5) == []
    assert T.all(d__s__length__double=3.0) == [t2]
    assert T.all(d__s__binary=b"foo") == [t2]


def test_select_function(t2: T) -> None:
    t2.save()
    assert T.get_fields(t2.pk, "s.length") == {"s.length": 3}
    assert T.get_fields(t2.pk, "s.length.double:n") == {"n": 3.0}
    assert T.get_fields(t2.pk, "s.binary") == {"s.binary": b"foo"}
    assert T.get_fields(t2.pk, "d.s.length") == {"d.s.length": 3}
    assert T.get_fields(t2.pk, "d.s.length.double:n") == {"n": 3.0}
    assert T.get_fields(t2.pk, "d.s.binary") == {"d.s.binary": b"foo"}


def test_select_invalid() -> None:
    class U(Model):
        s: str
        n: int
        b: bool

    Model.create_tables()
    error = "table 'u' has no column 'x' (available selectors are pk, s, n and b)"
    with pytest.raises(ValueError, match=re.escape(error)):
        U.get_fields(fields="x")
    with pytest.raises(ValueError, match=re.escape(error)):
        U.get(x__y=1)
    with pytest.raises(ValueError, match=re.escape(error)):
        U.all(order="x")
    error = "table 'u' has no column 'x' (available columns are pk, s, n and b)"
    with pytest.raises(ValueError, match=re.escape(error)):
        U.get(x=1)
    error = "column 'u.s' is not a JSON column"
    with pytest.raises(ValueError, match=re.escape(error)):
        U.get_fields(fields="s.x")
    with pytest.raises(ValueError, match=re.escape(error)):
        U.get(s__x=1)
    with pytest.raises(ValueError, match=re.escape(error)):
        U.all(order="s.x")
