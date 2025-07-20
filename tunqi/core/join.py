from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy
from sqlalchemy import Join

if TYPE_CHECKING:  # pragma: no cover
    from tunqi.core.table import Table

type Joins = dict[Table, list[Table]]
type Joined = sqlalchemy.Table | Join


def merge_joins(*joins: Joins) -> Joins:
    merged: Joins = {}
    for join in joins:
        for source, targets in join.items():
            if source not in merged:
                merged[source] = targets
            else:
                for target in targets:
                    if target not in merged[source]:
                        merged[source].append(target)
    return merged


def join(table: Table, joins: Joins) -> Joined:
    if table not in joins:
        return table.table
    clause: sqlalchemy.Table | Join = table.table
    for related_table in joins[table]:
        clause = clause.join(join(related_table, joins))
    return clause
