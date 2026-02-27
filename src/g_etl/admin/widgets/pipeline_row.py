"""Pipeline-rad widget med inline steg-prickar och statustext.

Varje dataset visas som en rad med 5 steg-indikatorer:
  ☑ Biotopskydd        ● ── ● ── ◐ ── ○ ── ○   staging: validerar geometri...
                       Ext  Stg  Mrt  Mrg  Exp

Klick pa en klarmarkerad prick (●) oppnar detaljpanelen for det steget.
"""

from enum import Enum

from rich.text import Text
from textual.events import Click
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

from g_etl.admin.models.dataset import Dataset


class StepState(Enum):
    """Status for ett enskilt pipeline-steg."""

    IDLE = "idle"  # ○  Ej startat
    RUNNING = "running"  # ◐  Pagar
    DONE = "done"  # ●  Klart
    FAILED = "failed"  # ✗  Fel
    SKIPPED = "skipped"  # ·  Hoppat over


# Steg-ordning
STEP_NAMES = ["Ext", "Stg", "Mrt", "Mrg", "Exp"]
STEP_COUNT = len(STEP_NAMES)

# Steg-index till schema-namn (for databasuppslag)
STEP_SCHEMAS = {
    0: "raw",  # Ext
    1: "staging_004",  # Stg
    2: "mart",  # Mrt
    3: "mart",  # Mrg (samma som mart, men fran warehouse)
    4: None,  # Exp (fil, inte schema)
}


class PipelineRow(Static):
    """En rad som visar ett dataset med inline pipeline-steg och statustext."""

    class SelectionChanged(Message):
        """Meddelande nar selection andras."""

        def __init__(self, row: "PipelineRow") -> None:
            super().__init__()
            self.row = row

    class StepClicked(Message):
        """Meddelande nar en klarmarkerad steg-prick klickas."""

        def __init__(self, dataset: Dataset, step_index: int) -> None:
            super().__init__()
            self.dataset = dataset
            self.step_index = step_index

    DEFAULT_CSS = """
    PipelineRow {
        height: 1;
        padding: 0 1;
    }

    PipelineRow:hover {
        background: $primary 15%;
    }

    PipelineRow.selected {
        background: $primary 30%;
    }

    PipelineRow.disabled {
        color: $text-muted;
    }

    PipelineRow.row-done {
        color: $text;
    }

    PipelineRow.row-failed {
        color: $text;
    }
    """

    selected: reactive[bool] = reactive(False)
    status_text: reactive[str] = reactive("")

    def __init__(self, dataset: Dataset, name_width: int = 22) -> None:
        super().__init__()
        self.dataset = dataset
        self.name_width = name_width
        self.steps: list[StepState] = [StepState.IDLE] * STEP_COUNT
        self.rows_count: int | None = None
        self.elapsed: float = 0.0
        self.error: str | None = None
        # Berakna steg-positioner (x-offset fran content-start)
        # " ☑ " (3) + namn (name_width) + "  " (2) = steg-start
        self._steps_start = 3 + name_width + 2
        # Varje steg: 1 char ikon + 4 chars " ── " (utom sista)
        # Steg-positioner: start, start+5, start+10, start+15, start+20
        self._step_stride = 5

    def on_mount(self) -> None:
        """Satt initial CSS-klass."""
        if not self.dataset.enabled:
            self.add_class("disabled")

    def on_click(self, event: Click) -> None:
        """Hantera klick — checkbox-zon eller steg-zon."""
        if not self.dataset.enabled:
            return

        # x relativt till widget-content (padding: 0 1 -> content borjar vid x=1)
        x = event.x - 1  # justera for vanster-padding

        # Kolla om klicket ar i steg-zonen
        step_index = self._hit_step(x)
        if step_index is not None and self.steps[step_index] in (
            StepState.DONE,
            StepState.FAILED,
        ):
            self.post_message(self.StepClicked(self.dataset, step_index))
            event.stop()
            return

        # Annars: toggle selection (klick i checkbox/namn-zonen)
        self.selected = not self.selected
        self.toggle_class("selected")
        self.post_message(self.SelectionChanged(self))

    def _hit_step(self, x: int) -> int | None:
        """Returnera steg-index om x traffar en steg-prick, annars None."""
        for i in range(STEP_COUNT):
            dot_x = self._steps_start + i * self._step_stride
            if abs(x - dot_x) <= 1:  # ±1 chars tolerans
                return i
        return None

    def set_step(self, step_index: int, state: StepState) -> None:
        """Uppdatera ett steg."""
        if 0 <= step_index < STEP_COUNT:
            self.steps[step_index] = state
            self.refresh()

    def set_status(self, text: str) -> None:
        """Satt statustext (visas till hoger om stegen)."""
        self.status_text = text
        self.refresh()

    def set_done(self, rows_count: int | None = None, elapsed: float = 0.0) -> None:
        """Markera hela raden som klar."""
        self.rows_count = rows_count
        self.elapsed = elapsed
        self.remove_class("row-failed")
        self.add_class("row-done")
        self.refresh()

    def set_failed(self, error: str) -> None:
        """Markera hela raden som felaktig."""
        self.error = error
        self.remove_class("row-done")
        self.add_class("row-failed")
        self.refresh()

    def render(self) -> Text:
        """Rendera raden: ☑ Namn  ● ── ● ── ◐ ── ○ ── ○  statustext."""
        text = Text()

        if not self.dataset.enabled:
            name = self.dataset.name[: self.name_width].ljust(self.name_width)
            text.append("   ", style="dim")
            text.append(name, style="dim")
            text.append("  \u00b7", style="dim")
            return text

        # Checkbox
        check = "\u2611" if self.selected else "\u2610"
        text.append(f" {check} ", style="bold" if self.selected else "")

        # Namn
        name = self.dataset.name[: self.name_width].ljust(self.name_width)
        text.append(name)

        # Steg-prickar:  ● ── ● ── ◐ ── ○ ── ○
        text.append("  ")
        for i, step in enumerate(self.steps):
            if i > 0:
                # Linje mellan steg
                line_style = "green" if self.steps[i - 1] == StepState.DONE else "dim"
                text.append(" \u2500\u2500 ", style=line_style)

            icon, style = _step_icon(step)
            text.append(icon, style=style)

        # Statustext
        text.append("   ")
        if self.error:
            err = self.error if len(self.error) <= 45 else self.error[:42] + "..."
            text.append(f"\u2717 {err}", style="red")
        elif self.rows_count is not None:
            text.append("\u2713 ", style="green")
            text.append(f"{self.rows_count:,} obj", style="green")
            if self.elapsed > 0:
                text.append(f" \u00b7 {self.elapsed:.1f}s", style="dim")
        elif self.status_text:
            text.append(self.status_text, style="cyan italic")
        elif all(s == StepState.IDLE for s in self.steps):
            pass  # Tomt = vantar

        return text


class TotalProgressBar(Static):
    """Visar total progress och sammanfattning under dataset-listan."""

    DEFAULT_CSS = """
    TotalProgressBar {
        height: 1;
        padding: 0 1;
    }
    """

    completed: reactive[int] = reactive(0)
    failed: reactive[int] = reactive(0)
    total: reactive[int] = reactive(0)
    elapsed: reactive[float] = reactive(0.0)
    total_rows: reactive[int] = reactive(0)
    finished: reactive[bool] = reactive(False)

    def render(self) -> Text:
        text = Text()

        if self.total == 0:
            return text

        done = self.completed + self.failed

        if self.finished:
            text.append("Klart: ", style="bold")
            if self.completed > 0:
                text.append(f"{self.completed} \u2713", style="green bold")
            if self.failed > 0:
                if self.completed > 0:
                    text.append("  ")
                text.append(f"{self.failed} \u2717", style="red bold")
            if self.total_rows > 0:
                text.append(f"   {self.total_rows:,} objekt totalt", style="")
            if self.elapsed > 0:
                mins = int(self.elapsed) // 60
                secs = int(self.elapsed) % 60
                text.append(f"   Tid: {mins:02d}:{secs:02d}", style="dim")
        else:
            bar_width = 30
            pct = done / self.total if self.total > 0 else 0
            filled = int(pct * bar_width)
            bar = "\u2588" * filled + "\u2591" * (bar_width - filled)

            text.append("Totalt: ", style="bold")
            text.append(bar, style="blue")
            text.append(f"  {done}/{self.total}", style="bold")
            text.append(f"  {int(pct * 100)}%", style="blue bold")
            if self.elapsed > 0:
                mins = int(self.elapsed) // 60
                secs = int(self.elapsed) % 60
                text.append(f"   Tid: {mins:02d}:{secs:02d}", style="dim")

        return text


class StepLegend(Static):
    """Visar steg-etiketter under steg-prickarna."""

    DEFAULT_CSS = """
    StepLegend {
        height: 1;
        padding: 0 1;
        color: $text-muted;
    }
    """

    def __init__(self, name_width: int = 22) -> None:
        super().__init__()
        self.name_width = name_width

    def render(self) -> Text:
        text = Text()
        # Prickar borjar vid offset 3+name_width+2, med stride 5.
        # Centrera 3-teckens etikett under 1-teckens prick: borja 1 pos fore.
        offset = 3 + self.name_width + 2 - 1
        text.append(" " * offset)
        for i, name in enumerate(STEP_NAMES):
            if i > 0:
                text.append("  ", style="dim")  # stride(5) - label(3) = 2
            text.append(name, style="dim italic")
        return text


def _step_icon(state: StepState) -> tuple[str, str]:
    """Returnera (ikon, rich-style) for ett steg."""
    return {
        StepState.IDLE: ("\u25cb", "dim"),
        StepState.RUNNING: ("\u25d0", "cyan bold"),
        StepState.DONE: ("\u25cf", "green"),
        StepState.FAILED: ("\u2717", "red bold"),
        StepState.SKIPPED: ("\u00b7", "dim"),
    }[state]
