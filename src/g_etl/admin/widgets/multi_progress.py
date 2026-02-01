"""Docker-stil multi-progress widget för pipeline-körning."""

from dataclasses import dataclass
from enum import Enum

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Static


class TaskStatus(Enum):
    """Status för en pipeline-task."""

    PENDING = "pending"  # ○ Väntande
    QUEUED = "queued"  # ◔ I kö
    RUNNING = "running"  # ⟳ Kör
    COMPLETED = "completed"  # ✓ Klar
    FAILED = "failed"  # ✗ Fel
    SKIPPED = "skipped"  # ⊘ Hoppad över


STATUS_ICONS = {
    TaskStatus.PENDING: ("○", "dim"),
    TaskStatus.QUEUED: ("◔", "yellow"),
    TaskStatus.RUNNING: ("⟳", "cyan bold"),
    TaskStatus.COMPLETED: ("✓", "green"),
    TaskStatus.FAILED: ("✗", "red bold"),
    TaskStatus.SKIPPED: ("⊘", "dim"),
}


@dataclass
class TaskProgress:
    """Representerar progress för en enskild task."""

    id: str
    name: str
    status: TaskStatus = TaskStatus.PENDING
    progress: float = 0.0  # 0.0 - 1.0
    total_items: int | None = None
    current_items: int = 0
    message: str = ""
    queue_position: int | None = None
    error: str | None = None
    rows_count: int | None = None


class TaskProgressRow(Static):
    """En rad som visar progress för en task."""

    DEFAULT_CSS = """
    TaskProgressRow {
        height: 1;
        width: 100%;
    }
    TaskProgressRow.running {
        background: $primary-darken-3;
    }
    TaskProgressRow.failed {
        background: $error-darken-3;
    }
    TaskProgressRow.completed {
        background: $success-darken-3;
    }
    """

    def __init__(self, task_data: TaskProgress, name_width: int = 26) -> None:
        super().__init__()
        self.task_data = task_data
        self.name_width = name_width

    def update_task(self, task_data: TaskProgress) -> None:
        """Uppdatera task och rendera om."""
        self.task_data = task_data
        self.refresh()
        # Uppdatera CSS-klass baserat på status
        self.remove_class("running", "failed", "completed", "queued")
        if task_data.status == TaskStatus.RUNNING:
            self.add_class("running")
        elif task_data.status == TaskStatus.FAILED:
            self.add_class("failed")
        elif task_data.status == TaskStatus.COMPLETED:
            self.add_class("completed")
        elif task_data.status == TaskStatus.QUEUED:
            self.add_class("queued")

    def render(self) -> Text:
        """Rendera task-raden."""
        task = self.task_data
        icon, style = STATUS_ICONS[task.status]

        # Bygg texten
        text = Text()
        text.append(f" {icon} ", style=style)

        # Namn (trunkerat om för långt)
        name = task.name[: self.name_width].ljust(self.name_width)
        name_style = "bold" if task.status == TaskStatus.RUNNING else ""
        text.append(name, style=name_style)

        # Progress bar (24 tecken bred)
        bar_width = 24
        if task.status == TaskStatus.PENDING:
            bar = " " * bar_width
        elif task.status == TaskStatus.COMPLETED:
            bar = "█" * bar_width
        elif task.status == TaskStatus.FAILED:
            filled = int(task.progress * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
        else:
            filled = int(task.progress * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)

        bar_style = {
            TaskStatus.RUNNING: "cyan",
            TaskStatus.COMPLETED: "green",
            TaskStatus.FAILED: "red",
            TaskStatus.QUEUED: "yellow dim",
        }.get(task.status, "dim")

        text.append(" [", style="dim")
        text.append(bar, style=bar_style)
        text.append("] ", style="dim")

        # Status-text
        if task.status == TaskStatus.COMPLETED and task.rows_count:
            text.append(f"{task.rows_count:>7} rader", style="green")
        elif task.status == TaskStatus.COMPLETED:
            text.append("      klar", style="green")
        elif task.status == TaskStatus.RUNNING:
            pct = int(task.progress * 100)
            text.append(f"  {pct:>3}%  ↓", style="cyan")
        elif task.status == TaskStatus.QUEUED and task.queue_position:
            text.append(f"   kö ({task.queue_position})", style="yellow")
        elif task.status == TaskStatus.FAILED:
            if task.error and len(task.error) > 15:
                err = task.error[:12] + "..."
            else:
                err = task.error or "FEL"
            text.append(f" {err}", style="red")
        elif task.status == TaskStatus.PENDING:
            text.append("  väntande", style="dim")
        elif task.status == TaskStatus.SKIPPED:
            text.append("  hoppades", style="dim")

        return text


class TotalProgressBar(Static):
    """Visar total progress för hela pipelinen."""

    DEFAULT_CSS = """
    TotalProgressBar {
        height: 1;
        padding: 0 1;
        background: $surface;
    }
    """

    completed = reactive(0)
    total = reactive(0)
    phase = reactive("Extract")

    def render(self) -> Text:
        """Rendera total-progress."""
        pct = self.completed / self.total if self.total > 0 else 0
        bar_width = 32
        filled = int(pct * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)

        text = Text()
        text.append(f" {self.phase}", style="bold")
        text.append(" " * (16 - len(self.phase)))
        text.append("[", style="dim")
        text.append(bar, style="blue")
        text.append("]", style="dim")
        text.append(f" {self.completed:>2}/{self.total:<2}", style="bold")
        text.append(f"  {int(pct * 100):>3}%", style="blue bold")

        return text


class StatusLine(Static):
    """Visar aktuellt meddelande."""

    DEFAULT_CSS = """
    StatusLine {
        height: 1;
        padding: 0 1;
        color: $text-muted;
        border-top: solid $primary-darken-2;
    }
    """

    message = reactive("")

    def render(self) -> Text:
        """Rendera statusraden."""
        text = Text()
        if self.message:
            text.append(" ⟳ ", style="cyan")
            text.append(self.message, style="italic")
        return text


class MultiProgressWidget(Widget):
    """Docker-stil multi-progress widget."""

    DEFAULT_CSS = """
    MultiProgressWidget {
        border: round $primary;
        height: auto;
        max-height: 24;
        min-height: 8;
    }

    MultiProgressWidget > Vertical {
        height: auto;
    }

    #progress-header {
        height: 2;
        padding: 0;
    }

    #task-list {
        height: auto;
        max-height: 18;
        overflow-y: auto;
    }
    """

    def __init__(self, title: str = "Pipeline") -> None:
        super().__init__()
        self.title = title
        self.tasks: dict[str, TaskProgress] = {}
        self.task_rows: dict[str, TaskProgressRow] = {}

    def compose(self) -> ComposeResult:
        """Bygg widgeten."""
        with Vertical():
            yield TotalProgressBar(id="total-progress")
            yield Vertical(id="task-list")
            yield StatusLine(id="status-line")

    def reset(self) -> None:
        """Återställ widgeten för ny körning."""
        self.tasks.clear()
        self.task_rows.clear()
        task_list = self.query_one("#task-list", Vertical)
        task_list.remove_children()
        self._update_total()
        self.set_status("")

    def add_task(self, task: TaskProgress) -> None:
        """Lägg till en ny task."""
        self.tasks[task.id] = task
        row = TaskProgressRow(task)
        self.task_rows[task.id] = row
        self.query_one("#task-list", Vertical).mount(row)
        self._update_total()

    def update_task(
        self,
        task_id: str,
        status: TaskStatus | None = None,
        progress: float | None = None,
        message: str | None = None,
        total_items: int | None = None,
        current_items: int | None = None,
        error: str | None = None,
        rows_count: int | None = None,
        queue_position: int | None = None,
    ) -> None:
        """Uppdatera en task."""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        if status is not None:
            task.status = status
        if progress is not None:
            task.progress = progress
        if message is not None:
            task.message = message
            self.set_status(f"{task.name}: {message}")
        if total_items is not None:
            task.total_items = total_items
        if current_items is not None:
            task.current_items = current_items
        if error is not None:
            task.error = error
        if rows_count is not None:
            task.rows_count = rows_count
        if queue_position is not None:
            task.queue_position = queue_position

        self.task_rows[task_id].update_task(task)
        self._update_total()

    def set_phase(self, phase: str) -> None:
        """Sätt fasbeskrivning (t.ex. 'Extract', 'Transform')."""
        total_bar = self.query_one("#total-progress", TotalProgressBar)
        total_bar.phase = phase

    def set_status(self, message: str) -> None:
        """Sätt statusmeddelande."""
        status = self.query_one("#status-line", StatusLine)
        status.message = message

    def _update_total(self) -> None:
        """Uppdatera total progress."""
        total_bar = self.query_one("#total-progress", TotalProgressBar)
        total_bar.total = len(self.tasks)
        total_bar.completed = sum(
            1
            for t in self.tasks.values()
            if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED)
        )
