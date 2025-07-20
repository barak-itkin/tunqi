from typing import Any

import pytest

from tunqi.sync import Database


@pytest.fixture
def t() -> dict[str, Any]:
    return {
        "columns": {
            "b": {
                "type": "boolean",
            },
            "n": {
                "type": "integer",
            },
            "x": {
                "type": "double",
            },
            "s": {
                "type": "string",
            },
            "o": {
                "type": "string",
                "nullable": True,
            },
            "dt": {
                "type": "datetime",
                "nullable": True,
            },
            "bs": {
                "type": "binary",
            },
            "d": {
                "type": "json",
            },
            "ns": {
                "type": "json",
                "index": True,
            },
            "ss": {
                "type": "json",
            },
            "f": {
                "type": "json",
            },
            "fs": {
                "type": "json",
            },
        }
    }


@pytest.fixture
def u() -> dict[str, Any]:
    return {
        "columns": {
            "s": {
                "type": "string:length",
                "unique": True,
                "length": 255,
            },
            "n": {
                "type": "integer",
                "nullable": True,
            },
            "b": {
                "type": "boolean",
                "nullable": True,
            },
        },
    }


@pytest.fixture
def u2() -> dict[str, Any]:
    return {
        "columns": {
            "n1": {
                "type": "integer",
            },
            "n2": {
                "type": "integer",
            },
            "s1": {
                "type": "string:length",
                "length": 255,
            },
            "s2": {
                "type": "string:length",
                "length": 255,
            },
        },
        "unique": [
            ["n1", "n2"],
            ["s1", "s2"],
        ],
    }


@pytest.fixture
def user() -> dict[str, Any]:
    return {
        "columns": {
            "name": {
                "type": "string",
            },
        }
    }


@pytest.fixture
def post() -> dict[str, Any]:
    return {
        "columns": {
            "user": {
                "type": "fk",
                "table": "user",
            },
            "content": {
                "type": "string",
            },
            "commentary": {
                "type": "backref",
                "table": "comment",
            },
            "tagging": {
                "type": "m2m",
                "table": "tag",
            },
        }
    }


@pytest.fixture
def comment() -> dict[str, Any]:
    return {
        "columns": {
            "post": {
                "type": "fk",
                "table": "post",
            },
            "content": {
                "type": "string",
            },
        }
    }


@pytest.fixture
def tag() -> dict[str, Any]:
    return {
        "columns": {
            "name": {
                "type": "string",
            },
            "posts": {
                "type": "m2m",
                "table": "post",
            },
        },
    }


@pytest.fixture
def db(db: Database, db_url: str, t: dict[str, Any]) -> Database:
    db.add_table("t", t)
    db.create_tables()
    return db
