import datetime as dt
import pathlib
from os import PathLike
from typing import Literal, override

from alembic import context
from alembic.autogenerate import RevisionContext
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.runtime.migration import MigrationContext, RevisionStep
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, MetaData
from sqlalchemy.sql.schema import SchemaItem

TEMPLATE = """
from alembic import op
import sqlalchemy as sa
{imports}

revision: str = {up_revision!r}
down_revision: str | None = {down_revision!r}


def upgrade() -> None:
    {upgrades}
"""


class Migration:

    def __init__(
        self,
        metadata: MetaData,
        migrations_directory: pathlib.Path,
        table_names: list[str] | None = None,
    ) -> None:
        self.metadata = metadata
        self.migrations_directory = migrations_directory
        self.table_names = table_names

    def make_migrations(self, connection: Connection) -> None:
        config = Config()
        script_directory = DynamicScriptDirectory(
            connection,
            self.metadata,
            self.migrations_directory,
            self.table_names,
        )
        revision_context = RevisionContext(
            config,
            script_directory,
            command_args=dict(
                message=None,
                autogenerate=True,
                sql=False,
                head="head",
                splice=False,
                branch_label=None,
                version_path=None,
                rev_id=None,
                depends_on=None,
            ),
            process_revision_directives=None,
        )

        def retrieve_migrations(rev: str, context: MigrationContext) -> list[RevisionStep]:
            revision_context.run_autogenerate(rev, context)
            return []

        with EnvironmentContext(
            config,
            script_directory,
            fn=retrieve_migrations,
            as_sql=False,
            template_args=revision_context.template_args,
            revision_context=revision_context,
        ):
            script_directory.run_env()
        if any(script.upgrade_ops and script.upgrade_ops.ops for script in revision_context.generated_revisions):
            for _ in revision_context.generate_scripts():
                pass

    def migrate(self, connection: Connection) -> None:
        config = Config()
        script_directory = DynamicScriptDirectory(
            connection,
            self.metadata,
            self.migrations_directory,
        )

        def upgrade(rev: str, context: MigrationContext) -> list[RevisionStep]:
            return script_directory._upgrade_revs("head", rev)

        with EnvironmentContext(
            config,
            script_directory,
            fn=upgrade,
            as_sql=False,
            starting_rev=None,
            destination_rev="head",
            tag=None,
        ):
            script_directory.run_env()


class DynamicScriptDirectory(ScriptDirectory):

    def __init__(
        self,
        connection: Connection,
        metadata: MetaData,
        migrations_directory: pathlib.Path,
        table_names: list[str] | None = None,
    ) -> None:
        self.connection = connection
        self.metadata = metadata
        self.migrations_directory = migrations_directory
        self.table_names = table_names
        super().__init__(
            str(self.migrations_directory),
            version_locations=[str(self.migrations_directory)],
        )

    @override
    def run_env(self) -> None:
        context.configure(connection=self.connection, target_metadata=self.metadata, include_object=self._include)
        with context.begin_transaction():
            context.run_migrations()
        self.connection.commit()

    @override
    def _rev_path(
        self,
        path: str | PathLike[str],
        rev_id: str,
        message: str | None,
        create_date: dt.datetime,
    ) -> pathlib.Path:
        migration_num = len(list(self.migrations_directory.iterdir())) + 1
        return self.migrations_directory / f"migration_{migration_num:03}.py"

    @override
    def _generate_template(self, src: pathlib.Path, dest: pathlib.Path, **kw) -> None:
        if not kw.get("imports"):
            kw["imports"] = ""
        output = TEMPLATE.format(**kw)
        dest.write_text(output)

    def _include(
        self,
        object: SchemaItem,
        name: str | None,
        type_: Literal["schema", "table", "column", "index", "unique_constraint", "foreign_key_constraint"],
        reflected: bool,
        compare_to: SchemaItem | None,
    ) -> bool:
        if type_ == "table" and self.table_names:
            return name in self.table_names
        return True
