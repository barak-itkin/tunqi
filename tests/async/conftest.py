import secrets
import shutil
from typing import Any, AsyncIterator, Awaitable

import pytest
import pytest_asyncio
from _pytest.capture import CaptureManager

from tunqi import AuditEvent, Database, Row
from tunqi.debug import console, print_event


@pytest.fixture
def db_name() -> str:
    return f"test_{secrets.token_hex(16)}"


@pytest_asyncio.fixture
async def db(
    pytestconfig: pytest.Config,
    request: pytest.FixtureRequest,
    db_url: str,
    db_name: str,
) -> AsyncIterator[Database]:
    # Pytest limits the terminal width to 80, which is annoying, and since this happens before any input, there's no way
    # to capture shutil.get_terminal_size().columns or sys.stdout in advance. So, we fetch the capture manager manually,
    # temporarily disable it, and set the width to the actual terminal size ourselves.
    capman: CaptureManager = pytestconfig.pluginmanager.getplugin("capturemanager")
    with capman.global_and_fixture_disabled():
        console.width = shutil.get_terminal_size().columns
    # Rather than printing the events for all tests, we collect them and only do so if a test fails.
    events: list[AuditEvent] = []

    def collect_event(event: AuditEvent) -> None:
        events.append(event)

    core_database = Database(db_url, default=True)
    if core_database.is_sqlite:
        database = core_database
    else:
        database = await core_database.create_database(db_name)
        database.set_default()
    with database.audit(collect_event):
        yield database
        await database.stop()
        if not core_database.is_sqlite:
            await core_database.drop_database(db_name)
            await core_database.stop()
    if request.node.report.failed:
        for event in events:
            print_event(event)


async def fields(items: Awaitable[list[Row]]) -> set[Any]:
    return {tuple(item.values()) if len(item) > 1 else item.popitem()[1] for item in await items}
