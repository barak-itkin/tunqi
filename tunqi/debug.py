from typing import Any

from rich.console import Console, ConsoleRenderable
from rich.padding import Padding
from rich.pretty import Pretty
from rich.rule import Rule
from rich.syntax import Syntax

from tunqi.audit import AuditEvent

console = Console(force_terminal=True, color_system="256")


def print_event(event: AuditEvent, start_time: float | None = None, indent: int = 0) -> None:
    if not start_time:
        start_time = event.start_time
        timestamp = str(event.start_datetime)
        separate = True
    else:
        offset = event.start_time - start_time
        timestamp = f"{offset:.6f}"
        separate = False
    if event.error:
        message = (
            f"[yellow]{timestamp}[/yellow] [red][bold]{event.name}[/bold] failed after {event.duration:.6f} seconds: "
            f"{event.error}"
        )
    else:
        message = f"[yellow]{timestamp}[/yellow] [bold]{event.name}[/bold] completed after {event.duration:.6f} seconds"
    print_(indent, message)
    data = event.data.copy()
    if "connection_id" in data and "connection_uid" in data:
        connection_id = data.pop("connection_id", None)
        connection_uid = data.pop("connection_uid", None)
        print_(indent, f"[blue]Connection {connection_uid} ({connection_id})[/blue]")
    if "statement" in data:
        statement = data.pop("statement")
        print_(indent, Syntax(statement, "sql", theme="monokai", word_wrap=True))
    if data:
        print_(indent, data)
    for child in event.children:
        print_event(child, start_time=start_time, indent=indent + 4)
    if separate:
        print_(0, Rule())


def print_(indent: int, data: Any) -> None:
    highlight = False if isinstance(data, str) else True
    if not isinstance(data, str | ConsoleRenderable):
        data = Pretty(data)
    console.print(Padding(data, (0, 0, 0, indent)), highlight=highlight)
