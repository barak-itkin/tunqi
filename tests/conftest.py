import base64
import contextlib
import datetime as dt
import pathlib
import subprocess
import time
from typing import Any, Generator

import pytest
from sqlalchemy import create_engine

from tunqi import Row
from tunqi.utils import and_

NOW = dt.datetime.now().astimezone()
LATER = NOW + dt.timedelta(hours=1)
EARLIER = NOW - dt.timedelta(hours=1)
DIALECTS = "sqlite", "postgresql", "mysql"

SQLITE_URL = "sqlite:///{path}"
POSTGRESQL_CREDS = {
    "username": "user",
    "password": "1234",
    "port": "5432",
    "container": "storm-test-postgresql",
}
POSTGRESQL_URL = "postgresql://{username}:{password}@localhost:{port}/"
POSTGRESQL_COMMAND = (
    "docker run --name {container} "
    "-e POSTGRES_PASSWORD={password} "
    "-e POSTGRES_USER={username} "
    "-p {port}:{port} "
    "-d postgres:latest"
)
MYSQL_CREDS = {
    "username": "root",
    "password": "1234",
    "port": "3306",
    "container": "storm-test-mysql",
}
MYSQL_URL = "mysql://{username}:{password}@localhost:{port}/"
MYSQL_COMMAND = (
    "docker run --name {container} "
    "-e MYSQL_ROOT_PASSWORD={password} "
    "-e MYSQL_PASSWORD={password} "
    "-p {port}:{port} "
    "-d mysql:latest"
)


def pytest_addoption(parser):
    parser.addoption(
        "--dialects",
        action="store",
        default=",".join(DIALECTS),
        help=f"Comma-separated list of dialects to test (available dialects are {and_(DIALECTS)})",
    )


def pytest_configure(config):
    dialects = config.getoption("--dialects").split(",")
    invalid = [dialect for dialect in dialects if dialect not in DIALECTS]
    if invalid:
        raise pytest.UsageError(f"Invalid dialects {and_(invalid)} (available dialects are {and_(DIALECTS)})")
    if is_main_process(config) and "postgresql" in DIALECTS:
        run_container("postgresql", POSTGRESQL_CREDS, POSTGRESQL_COMMAND, POSTGRESQL_URL)
    if is_main_process(config) and "mysql" in DIALECTS:
        run_container("mysql", MYSQL_CREDS, MYSQL_COMMAND, MYSQL_URL)


def pytest_unconfigure(config):
    if is_main_process(config):
        remove_container(POSTGRESQL_CREDS["container"])
        remove_container(MYSQL_CREDS["container"])


def is_main_process(config):
    return not hasattr(config, "workerinput")


def run_container(name: str, credentials: dict[str, Any], command: str, url: str) -> None:
    try:
        subprocess.run(command.format(**credentials), shell=True, check=True, capture_output=True)
    except Exception:
        time.sleep(2)
    if not connect(url.format(**credentials)):
        pytest.skip(f"failed to connect to {name}")


def remove_container(name: str) -> None:
    with contextlib.suppress(Exception):
        subprocess.run(f"docker rm -f {name}", shell=True, check=True, capture_output=True)


def connect(url: str, timeout: int = 30) -> bool:
    if url.startswith("postgresql"):
        url = url.replace("postgresql", "postgresql+psycopg2", 1)
    elif url.startswith("mysql"):
        url = url.replace("mysql", "mysql+pymysql", 1)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            engine = create_engine(url)
            with engine.connect():
                pass
            engine.dispose()
            return True
        except Exception:
            time.sleep(0.5)
    return False


# Save a reference to the test report so we can know if it failed (e.g. to print audit events only on failure).
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, pytest.CallInfo, None]:
    outcome = yield
    setattr(item, "report", outcome.get_result())  # type: ignore


@pytest.fixture(
    params=[
        ("b", True, True),
        ("b", False, False),
        ("b__is", True, True),
        ("b__is", False, False),
        ("b__is_not", True, False),
        ("b__is_not", False, True),
        ("n", 1, True),
        ("n", 0, False),
        ("n__ne", 1, False),
        ("n__ne", 0, True),
        ("n__lt", 2, True),
        ("n__lt", 0, False),
        ("n__lt", 0, False),
        ("n__le", 2, True),
        ("n__le", 1, True),
        ("n__le", 0, False),
        ("n__gt", 0, True),
        ("n__gt", 1, False),
        ("n__gt", 2, False),
        ("n__ge", 0, True),
        ("n__ge", 1, True),
        ("n__ge", 2, False),
        ("n__in", [1, 2], True),
        ("n__in", [3, 4], False),
        ("n__not_in", [1, 2], False),
        ("n__not_in", [3, 4], True),
        ("x", 1.0, True),
        ("x", 1.1, False),
        ("x__ne", 1.1, True),
        ("x__ne", 1.0, False),
        ("x__lt", 1.1, True),
        ("x__lt", 1.0, False),
        ("x__lt", 0.9, False),
        ("x__le", 1.1, True),
        ("x__le", 1.0, True),
        ("x__le", 0.9, False),
        ("x__gt", 0.9, True),
        ("x__gt", 1.0, False),
        ("x__gt", 1.1, False),
        ("x__ge", 0.9, True),
        ("x__ge", 1.0, True),
        ("x__ge", 1.1, False),
        ("s", "foo", True),
        ("s", "bar", False),
        ("s__ne", "foo", False),
        ("s__ne", "bar", True),
        ("s__contains", "oo", True),
        ("s__contains", "ar", False),
        ("s__startswith", "f", True),
        ("s__startswith", "b", False),
        ("s__endswith", "o", True),
        ("s__endswith", "r", False),
        ("s__like", "f%", True),
        ("s__not_like", "b%", True),
        ("s__matches", "^f.*", True),
        ("s__matches", ".*r$", False),
        ("s__in", ["foo", "bar"], True),
        ("s__in", ["one", "two"], False),
        ("s__not_in", ["foo", "bar"], False),
        ("s__not_in", ["one", "two"], True),
        ("o__is", None, True),
        ("o__is_not", None, False),
        ("d__has", "b", True),
        ("d__has", "b2", False),
        ("d__b", True, True),
        ("d__n", 1, True),
        ("d__x", 1.0, True),
        ("d__s", "foo", True),
        ("d__b2", False, False),
        ("d__n2", 2, False),
        ("d__x2", 2.0, False),
        ("d__s2", "bar", False),
        ("dt__lt", EARLIER, False),
        ("dt__lt", NOW, False),
        ("dt__lt", LATER, True),
        ("dt__le", EARLIER, False),
        ("dt__le", NOW, True),
        ("dt__le", LATER, True),
        ("dt__gt", EARLIER, True),
        ("dt__gt", NOW, False),
        ("dt__gt", LATER, False),
        ("dt__ge", EARLIER, True),
        ("dt__ge", NOW, True),
        ("dt__ge", LATER, False),
        ("bs", b"\x01\x02", True),
        ("bs", b"\x03\x04", False),
        ("bs__ne", b"\x01\x02", False),
        ("bs__ne", b"\x03\x04", True),
        ("d__dt__datetime", NOW.isoformat(), True),
        ("d__dt__datetime__gt", EARLIER.isoformat(), True),
        ("d__dt__datetime__lt", LATER.isoformat(), True),
        ("d__bs__bytes", base64.b64encode(b"\x01\x02").decode(), True),
        ("ns__contains", 1, True),
        ("ns__contains", 2, True),
        ("ns__contains", 3, False),
        ("ss__contains", "foo", True),
        ("ss__contains", "bar", True),
        ("ss__contains", "foobar", False),
        ("f__s", "foo", True),
        ("f__s", "bar", False),
        ("f__s__startswith", "f", True),
        ("f__s__startswith", "b", False),
        ("fs__0__s", "foo", True),
        ("fs__0__s", "bar", False),
        ("fs__1__s", "foo", False),
        ("fs__1__s", "bar", True),
        ("fs__2__s__ne", "foo", True),
        ("fs__has", "0.s", True),
        ("fs__has", "0.x", False),
        ("fs__has", "3.s", False),
    ]
)
def condition(request: pytest.FixtureRequest) -> tuple[str, Any, bool]:
    return request.param


@pytest.fixture(params=DIALECTS)
def db_url(request: pytest.FixtureRequest, tmp_path: pathlib.Path) -> str:
    dialects = request.config.getoption("--dialects").split(",")
    if request.param not in dialects:
        pytest.skip(f"{request.param} is not selected")
    if request.param == "sqlite":
        path = tmp_path / "storm-test.db"
        return SQLITE_URL.format(path=path)
    if request.param == "postgresql":
        # run_container(request.param, POSTGRESQL_CREDS, POSTGRESQL_COMMAND, POSTGRESQL_URL)
        return POSTGRESQL_URL.format(**POSTGRESQL_CREDS)
    if request.param == "mysql":
        # run_container(request.param, MYSQL_CREDS, MYSQL_COMMAND, MYSQL_URL)
        return MYSQL_URL.format(**MYSQL_CREDS)
    raise ValueError(f"invalid dialect {request.param}")


@pytest.fixture
def r1() -> Row:
    return {
        "b": False,
        "n": 0,
        "x": 0.0,
        "s": "",
        "o": None,
        "dt": None,
        "bs": b"",
        "d": {},
        "ns": [],
        "ss": [],
        "f": None,
        "fs": [],
    }


@pytest.fixture
def r2() -> Row:
    return {
        "b": True,
        "n": 1,
        "x": 1.0,
        "s": "foo",
        "o": None,
        "dt": NOW,
        "bs": b"\x01\x02",
        "d": {"b": True, "n": 1, "x": 1.0, "s": "foo", "dt": NOW, "bs": b"\x01\x02"},
        "ns": [1, 2],
        "ss": ["foo", "bar"],
        "f": {"s": "foo"},
        "fs": [{"s": "foo"}, {"s": "bar"}, {"s": None}],
    }


@pytest.fixture
def rs(r1: Row) -> list[Row]:
    return [
        r1
        | {
            "n": i,
            "b": i % 2 == 0,
            "d": {"x": i},
        }
        for i in range(10)
    ]
