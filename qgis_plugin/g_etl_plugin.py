"""G-ETL QGIS Plugin - Huvudklass."""

from pathlib import Path
from typing import List, Tuple

from qgis.core import (
    Qgis,
    QgsApplication,
    QgsProject,
    QgsTask,
    QgsVectorLayer,
)
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox


class GETLPlugin:
    """G-ETL QGIS Plugin."""

    def __init__(self, iface):
        """Initiera plugin.

        Args:
            iface: QgisInterface för att interagera med QGIS.
        """
        self.iface = iface
        self.plugin_dir = Path(__file__).parent
        self.actions = []
        self.menu_name = "G-ETL"

        # Runner skapas efter dependency-check
        self.runner = None

    def initGui(self):
        """Skapa GUI-element (meny, toolbar)."""
        # Huvudåtgärd - kör pipeline
        icon_path = self.plugin_dir / "icon.png"
        icon = QIcon(str(icon_path)) if icon_path.exists() else QIcon()

        self.run_action = QAction(icon, "Kör G-ETL Pipeline...", self.iface.mainWindow())
        self.run_action.setToolTip("Kör ETL-pipeline för geodata")
        self.run_action.triggered.connect(self.run_pipeline_dialog)

        # Lägg till i toolbar och meny
        self.iface.addToolBarIcon(self.run_action)
        self.iface.addPluginToMenu(self.menu_name, self.run_action)
        self.actions.append(self.run_action)

        # Info-åtgärd
        self.info_action = QAction("Om G-ETL", self.iface.mainWindow())
        self.info_action.triggered.connect(self.show_about)
        self.iface.addPluginToMenu(self.menu_name, self.info_action)
        self.actions.append(self.info_action)

    def unload(self):
        """Ta bort GUI-element."""
        for action in self.actions:
            self.iface.removePluginMenu(self.menu_name, action)
            self.iface.removeToolBarIcon(action)

    def _ensure_dependencies(self) -> bool:
        """Kontrollera och installera dependencies vid behov.

        Returns:
            True om dependencies finns/installerades.
        """
        from .deps import (
            check_dependencies,
            ensure_dependencies,
            get_install_command,
            install_duckdb_extensions,
        )

        missing = check_dependencies()

        if missing:
            from .dialog import DependencyDialog

            dialog = DependencyDialog(missing, self.iface.mainWindow())

            if dialog.exec_():
                # Användaren accepterade installation
                dialog.set_installing("Installerar paket...")

                def on_progress(msg: str):
                    dialog.set_installing(msg)

                success = ensure_dependencies(on_progress)

                if not success:
                    dialog.set_complete(False, "Installation misslyckades")
                    install_cmd = get_install_command(missing)
                    QMessageBox.critical(
                        self.iface.mainWindow(),
                        "G-ETL",
                        "Kunde inte installera nödvändiga paket.\n\n"
                        "Öppna Terminal och kör:\n"
                        f"{install_cmd}\n\n"
                        "Starta sedan om QGIS.",
                    )
                    return False

                # Installera DuckDB extensions
                dialog.set_installing("Installerar DuckDB extensions...")
                install_duckdb_extensions(on_progress)

                dialog.set_complete(True, "Installation klar!")
            else:
                # Användaren avbröt
                return False

        return True

    def _get_runner(self):
        """Hämta eller skapa runner (lazy loading)."""
        if self.runner is None:
            from .qgis_runner import QGISPipelineRunner

            self.runner = QGISPipelineRunner(self.plugin_dir)
        return self.runner

    def run_pipeline_dialog(self):
        """Visa dialog för att köra pipeline."""
        # Kontrollera dependencies först
        if not self._ensure_dependencies():
            return

        try:
            runner = self._get_runner()
            datasets = runner.list_datasets()
        except Exception as e:
            QMessageBox.critical(
                self.iface.mainWindow(),
                "G-ETL",
                f"Kunde inte ladda datasets:\n{e}",
            )
            return

        if not datasets:
            QMessageBox.warning(
                self.iface.mainWindow(),
                "G-ETL",
                "Inga datasets konfigurerade.\nKontrollera config/datasets.yml",
            )
            return

        # Visa dataset-dialog
        from .dialog import DatasetDialog

        dialog = DatasetDialog(datasets, self.iface.mainWindow())

        if dialog.exec_():
            selected = dialog.get_selected_datasets()
            if not selected:
                QMessageBox.warning(
                    self.iface.mainWindow(),
                    "G-ETL",
                    "Inga datasets valda.",
                )
                return

            export_format = dialog.get_export_format()
            output_dir = dialog.get_output_dir()
            phases = dialog.get_phases()

            # Starta pipeline i bakgrundstråd
            self._run_pipeline(selected, output_dir, export_format, phases)

    def _run_pipeline(
        self,
        dataset_ids: List[str],
        output_dir: Path,
        export_format: str,
        phases: Tuple[bool, bool, bool],
    ):
        """Kör pipeline i bakgrundstråd."""
        from .dialog import ProgressDialog

        progress_dialog = ProgressDialog(self.iface.mainWindow())

        # Skapa task
        task = PipelineTask(
            runner=self._get_runner(),
            dataset_ids=dataset_ids,
            output_dir=output_dir,
            export_format=export_format,
            phases=phases,
            progress_dialog=progress_dialog,
        )

        # Callback när klart
        task.taskCompleted.connect(lambda: self._on_pipeline_complete(task, progress_dialog))
        task.taskTerminated.connect(lambda: self._on_pipeline_failed(task, progress_dialog))

        # Starta task
        QgsApplication.taskManager().addTask(task)

        # Visa progress-dialog
        progress_dialog.exec_()

    def _on_pipeline_complete(self, task: "PipelineTask", dialog):
        """Hantera lyckad pipeline."""
        if task.output_path and task.output_path.exists():
            dialog.set_complete(True, "Pipeline klar!")
            dialog.add_log(f"\nOutput: {task.output_path}")

            # Fråga om att ladda till QGIS
            reply = QMessageBox.question(
                self.iface.mainWindow(),
                "G-ETL",
                "Pipeline klar!\n\nVill du ladda resultatet till kartan?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes,
            )

            if reply == QMessageBox.Yes:
                self._load_layer(task.output_path)
        else:
            dialog.set_complete(False, "Pipeline slutförd men ingen output skapad")

    def _on_pipeline_failed(self, task: "PipelineTask", dialog):
        """Hantera misslyckad pipeline."""
        error_msg = getattr(task, "error_message", "Okänt fel")
        dialog.set_complete(False, f"Pipeline misslyckades: {error_msg}")

    def _load_layer(self, layer_path: Path):
        """Ladda exporterat lager till QGIS."""
        layer_name = f"G-ETL {layer_path.stem}"
        layer = QgsVectorLayer(str(layer_path), layer_name, "ogr")

        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
            self.iface.messageBar().pushMessage(
                "G-ETL",
                f"Laddade {layer.featureCount()} features",
                level=Qgis.Success,
                duration=5,
            )
            # Zooma till lagret
            self.iface.mapCanvas().setExtent(layer.extent())
            self.iface.mapCanvas().refresh()
        else:
            QMessageBox.warning(
                self.iface.mainWindow(),
                "G-ETL",
                f"Kunde inte ladda lagret:\n{layer_path}",
            )

    def show_about(self):
        """Visa om-dialog."""
        # Läs version från metadata
        metadata_path = self.plugin_dir / "metadata.txt"
        version = "dev"
        if metadata_path.exists():
            for line in metadata_path.read_text().splitlines():
                if line.startswith("version="):
                    version = line.split("=", 1)[1]
                    break

        QMessageBox.about(
            self.iface.mainWindow(),
            "Om G-ETL",
            f"<h3>G-ETL v{version}</h3>"
            f"<p>ETL-pipeline för svenska geodata med H3-indexering.</p>"
            f"<p>Hämta data från WFS, Lantmäteriet, GeoParquet m.fl. "
            f"och transformera till H3-celler för analys.</p>"
            f"<p><a href='https://github.com/10gbrand/g-etl'>GitHub</a></p>",
        )


class PipelineTask(QgsTask):
    """Bakgrundsuppgift för pipeline-körning."""

    def __init__(
        self,
        runner,
        dataset_ids: List[str],
        output_dir: Path,
        export_format: str,
        phases: Tuple[bool, bool, bool],
        progress_dialog,
    ):
        super().__init__("G-ETL Pipeline", QgsTask.CanCancel)
        self.runner = runner
        self.dataset_ids = dataset_ids
        self.output_dir = output_dir
        self.export_format = export_format
        self.phases = phases
        self.progress_dialog = progress_dialog
        self.output_path = None
        self.error_message = None

    def run(self):
        """Kör pipeline (i bakgrundstråd)."""
        try:

            def on_progress(message: str, percent: float):
                # Anropas från worker-tråd, använd thread-safe metod
                self.setProgress(percent)
                # OBS: Vi kan inte uppdatera GUI härifrån direkt
                # Men QgsTask hanterar progress via setProgress()

            def on_log(message: str):
                # Logga till konsol (GUI-uppdatering sker via signals)
                print(f"[G-ETL] {message}")

            self.output_path = self.runner.run_pipeline(
                dataset_ids=self.dataset_ids,
                output_dir=self.output_dir,
                export_format=self.export_format,
                phases=self.phases,
                on_progress=on_progress,
                on_log=on_log,
            )

            return self.output_path is not None

        except Exception as e:
            self.error_message = str(e)
            return False

    def cancel(self):
        """Avbryt pipeline."""
        # TODO: Implementera avbrytning i PipelineRunner
        super().cancel()

    def finished(self, result: bool):
        """Anropas på huvudtråden när klart."""
        # Uppdatera dialog
        if result and self.output_path:
            self.progress_dialog.set_complete(True, "Pipeline klar!")
            self.progress_dialog.add_log(f"Output: {self.output_path}")
        else:
            error = self.error_message or "Okänt fel"
            self.progress_dialog.set_complete(False, f"Fel: {error}")
            self.progress_dialog.add_log(f"Fel: {error}")
