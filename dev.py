import contextlib
import functools
import http.server
import pathlib
import re
import shutil
import subprocess
import tomllib
from typing import Any

import click

ROOT = pathlib.Path(__file__).parent
PACKAGE: str = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["name"]
LINE_LENGTH = 120
COVERAGE_PORT = 8888
ARTEFACTS = [
    ".pytest_cache",
    ".coverage",
    "htmlcov",
    ".mypy_cache",
]


@click.group()
def main() -> None:
    pass


@main.command()
def clean() -> None:
    for path in ROOT.rglob("*"):
        if path.name not in ARTEFACTS:
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


@main.command()
@click.argument("args", nargs=-1)
def test(args: list[str]) -> None:
    tests = []
    for arg in args:
        tests.extend(["-k", arg])
    _execute("pytest", "tests", "-n", "auto", *tests)


@main.command()
def cov() -> None:
    _execute("pytest", f"--cov={PACKAGE}", "--cov-report=html", "tests")
    _serve(ROOT / "htmlcov", COVERAGE_PORT)


@main.command()
@click.argument("args", nargs=-1)
def lint(args: list[str]) -> None:
    paths = []
    for arg in args:
        path = ROOT / PACKAGE / arg.replace(".", "/")
        if not path.exists():
            path = path.with_suffix(".py")
        if not path.exists():
            raise FileNotFoundError(f"{path} does not exist")
        paths.append(path)
    if not paths:
        paths.extend([ROOT / PACKAGE, ROOT / "tests"])
    for path in paths:
        _execute("black", f"--line-length={LINE_LENGTH}", path)
        _execute("isort", "--profile=black", path)
        _execute("flake8", f"--max-line-length={LINE_LENGTH}", "--extend-ignore=E203", path)


@main.command()
@click.argument("args", nargs=-1)
def type(args: list[str]) -> None:
    packages = []
    for arg in args:
        packages.extend(["-p", f"{PACKAGE}.{arg}"])
    if not packages:
        packages.extend(["-p", PACKAGE, "-p", "tests"])
    _execute("mypy", *packages)


@main.command()
def sync() -> None:
    filenames = ["database.py", "model.py", "model_type.py", "fk.py", "backref.py", "m2m.py"]
    async_paths: list[pathlib.Path] = []
    for path in (ROOT / PACKAGE).rglob("*.py"):
        if path.name in filenames and path.parent.name != "sync":
            async_paths.append(path)
    for async_path in async_paths:
        async_code = async_path.read_text()
        sync_code = _async_to_sync(async_code, async_paths)
        sync_path = ROOT / PACKAGE / "sync" / async_path.name
        sync_path.write_text(sync_code)
    async_tests_directory = ROOT / "tests" / "async"
    sync_tests_directory = ROOT / "tests" / "sync"
    for async_path in async_tests_directory.rglob("*.py"):
        sync_path = sync_tests_directory / async_path.relative_to(async_tests_directory)
        sync_path.parent.mkdir(parents=True, exist_ok=True)
        sync_path.write_text(_async_to_sync(async_path.read_text(), async_paths))


def _execute(*args: Any) -> None:
    subprocess.run([str(arg) for arg in args])


def _serve(directory: pathlib.Path, port: int) -> None:
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler,
        directory=str(directory),
    )
    server = http.server.HTTPServer(("localhost", port), handler)
    print(f"http://localhost:{port}")
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


def _async_to_sync(text: str, async_paths: list[pathlib.Path]) -> str:
    # Change async imports to sync equivalents.
    text = text.replace(f"from {PACKAGE} import", f"from {PACKAGE}.sync import")
    for path in async_paths:
        relative = path.relative_to(ROOT / PACKAGE)
        import_path = str(relative).removesuffix(".py").replace("/", ".")
        text = text.replace(f"from {PACKAGE}.{import_path} import", f"from {PACKAGE}.sync.{relative.stem} import")
    text = text.replace("from sqlalchemy.ext.asyncio import", "from sqlalchemy import")
    # Change async drivers to sync equivalents.
    text = text.replace("+aiosqlite", "")
    text = text.replace("asyncpg", "psycopg2")
    text = text.replace("aiomysql", "pymysql")
    text = text.replace("asyncmy", "mariadbconnector")
    # Change async engine quirks to standard usage.
    text = text.replace("self.engine.sync_engine", "self.engine")
    text = re.sub(
        r"async with (.*?)\.begin\(\) as connection:\n\s*"
        r"await connection\.run_sync\(self\.metadata(.*?)(, .*?)?\)",
    r"self.metadata\2(\1\3)", text)
    text = re.sub(r"connection\.run_sync\(migration(.*?)\)", r"migration\1(connection)", text)
    # Remove pytest_asyncio import and replace references to it with pytest.
    if "import pytest\n" in text:
        text = text.replace("import pytest_asyncio\n", "")
    text = text.replace("pytest_asyncio", "pytest")
    # Remove pytest_asyncio mark.
    text = re.sub("\n*pytestmark = pytest.mark.asyncio", "", text)
    # Remove import pytest if it's left unused.
    if "import pytest" in text and text.count("pytest") == 1:
        text = text.replace("import pytest\n", "")
    # async def -> def
    # async for -> for
    # async with -> with
    # asynccontextmanager -> contextmanager
    # await ... -> ...
    # AsyncIterator -> Iterator
    # AsyncDatabase -> Database
    # create_async_engine -> create_engine
    text = re.sub(r"([aA]sync_?|await) *", "", text)
    # Awaitable ->
    # Awaitable[...] -> ...
    text = re.sub(r"Awaitable\[(.*?)\]", r"\1", text)
    text = re.sub(r"(,\s*)?Awaitable", "", text)
    return text


if __name__ == "__main__":
    main()
