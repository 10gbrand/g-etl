"""Qt-dialoger för G-ETL QGIS Plugin."""

from pathlib import Path

from qgis.PyQt.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class DependencyDialog(QDialog):
    """Dialog för installation av dependencies."""

    def __init__(self, missing_packages: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("G-ETL - Installera beroenden")
        self.setMinimumWidth(400)

        layout = QVBoxLayout()

        # Info-text
        info = QLabel(
            f"G-ETL behöver installera följande Python-paket:\n\n"
            f"{', '.join(missing_packages)}\n\n"
            f"Detta görs en gång och tar några sekunder."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        # Progress
        self.progress_label = QLabel("")
        layout.addWidget(self.progress_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.hide()
        layout.addWidget(self.progress_bar)

        # Knappar
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

        self.setLayout(layout)

    def set_installing(self, message: str):
        """Visa installation pågår."""
        self.progress_label.setText(message)
        self.progress_bar.show()
        self.button_box.setEnabled(False)

    def set_complete(self, success: bool, message: str):
        """Visa resultat."""
        self.progress_label.setText(message)
        self.progress_bar.hide()
        self.button_box.setEnabled(True)
        if success:
            self.button_box.button(QDialogButtonBox.Ok).setFocus()


class DatasetDialog(QDialog):
    """Huvuddialog för att välja datasets och köra pipeline."""

    def __init__(self, datasets: list[dict], parent=None):
        super().__init__(parent)
        self.setWindowTitle("G-ETL Pipeline")
        self.setMinimumSize(500, 600)

        self.datasets = datasets
        self.dataset_checks: dict[str, QCheckBox] = {}

        layout = QVBoxLayout()

        # Dataset-väljare
        dataset_group = QGroupBox("Välj datasets")
        dataset_layout = QVBoxLayout()

        # Scroll area för många datasets
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout()

        # Gruppera efter typ om tillgängligt
        datasets_by_type: dict[str, list[dict]] = {}
        for ds in datasets:
            ds_type = ds.get("type", "Övriga")
            if ds_type not in datasets_by_type:
                datasets_by_type[ds_type] = []
            datasets_by_type[ds_type].append(ds)

        for ds_type, type_datasets in sorted(datasets_by_type.items()):
            # Typ-rubrik
            type_label = QLabel(f"<b>{ds_type}</b>")
            scroll_layout.addWidget(type_label)

            for ds in type_datasets:
                ds_id = ds.get("id", "")
                ds_name = ds.get("name", ds_id)
                cb = QCheckBox(f"{ds_name} ({ds_id})")
                cb.setToolTip(ds.get("description", ""))
                self.dataset_checks[ds_id] = cb
                scroll_layout.addWidget(cb)

            scroll_layout.addSpacing(10)

        scroll_layout.addStretch()
        scroll_widget.setLayout(scroll_layout)
        scroll.setWidget(scroll_widget)
        dataset_layout.addWidget(scroll)

        # Välj alla / Avmarkera alla
        btn_layout = QHBoxLayout()
        select_all_btn = QPushButton("Välj alla")
        select_all_btn.clicked.connect(self._select_all)
        btn_layout.addWidget(select_all_btn)

        deselect_all_btn = QPushButton("Avmarkera alla")
        deselect_all_btn.clicked.connect(self._deselect_all)
        btn_layout.addWidget(deselect_all_btn)

        btn_layout.addStretch()
        dataset_layout.addLayout(btn_layout)

        dataset_group.setLayout(dataset_layout)
        layout.addWidget(dataset_group)

        # Inställningar
        settings_group = QGroupBox("Inställningar")
        settings_layout = QVBoxLayout()

        # Export-format
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("Exportformat:"))
        self.format_combo = QComboBox()
        self.format_combo.addItems(
            [
                "GeoPackage (.gpkg)",
                "GeoParquet (.parquet)",
                "FlatGeobuf (.fgb)",
            ]
        )
        format_layout.addWidget(self.format_combo)
        format_layout.addStretch()
        settings_layout.addLayout(format_layout)

        # Output-katalog
        output_layout = QHBoxLayout()
        output_layout.addWidget(QLabel("Output-katalog:"))
        self.output_edit = QLineEdit()
        self.output_edit.setText(str(Path.home() / "g-etl-output"))
        output_layout.addWidget(self.output_edit)
        browse_btn = QPushButton("Bläddra...")
        browse_btn.clicked.connect(self._browse_output)
        output_layout.addWidget(browse_btn)
        settings_layout.addLayout(output_layout)

        # Transform-faser
        phases_layout = QHBoxLayout()
        phases_layout.addWidget(QLabel("Faser:"))
        self.staging_check = QCheckBox("Staging")
        self.staging_check.setChecked(True)
        phases_layout.addWidget(self.staging_check)
        self.staging2_check = QCheckBox("Staging 2")
        self.staging2_check.setChecked(True)
        phases_layout.addWidget(self.staging2_check)
        self.mart_check = QCheckBox("Mart")
        self.mart_check.setChecked(True)
        phases_layout.addWidget(self.mart_check)
        phases_layout.addStretch()
        settings_layout.addLayout(phases_layout)

        settings_group.setLayout(settings_layout)
        layout.addWidget(settings_group)

        # Knappar
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Ok).setText("Kör pipeline")
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

        self.setLayout(layout)

    def _select_all(self):
        """Markera alla datasets."""
        for cb in self.dataset_checks.values():
            cb.setChecked(True)

    def _deselect_all(self):
        """Avmarkera alla datasets."""
        for cb in self.dataset_checks.values():
            cb.setChecked(False)

    def _browse_output(self):
        """Öppna katalog-dialog."""
        path = QFileDialog.getExistingDirectory(
            self,
            "Välj output-katalog",
            self.output_edit.text(),
        )
        if path:
            self.output_edit.setText(path)

    def get_selected_datasets(self) -> list[str]:
        """Hämta valda dataset-ID:n."""
        return [ds_id for ds_id, cb in self.dataset_checks.items() if cb.isChecked()]

    def get_export_format(self) -> str:
        """Hämta valt exportformat."""
        formats = {
            0: "gpkg",
            1: "geoparquet",
            2: "fgb",
        }
        return formats.get(self.format_combo.currentIndex(), "gpkg")

    def get_output_dir(self) -> Path:
        """Hämta output-katalog."""
        return Path(self.output_edit.text())

    def get_phases(self) -> tuple[bool, bool, bool]:
        """Hämta valda transform-faser."""
        return (
            self.staging_check.isChecked(),
            self.staging2_check.isChecked(),
            self.mart_check.isChecked(),
        )


class ProgressDialog(QDialog):
    """Dialog för att visa pipeline-progress."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("G-ETL - Kör pipeline")
        self.setMinimumSize(500, 400)
        self.setModal(True)

        layout = QVBoxLayout()

        # Status
        self.status_label = QLabel("Startar...")
        self.status_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.status_label)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        # Logg
        log_label = QLabel("Logg:")
        layout.addWidget(log_label)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("font-family: monospace;")
        layout.addWidget(self.log_text)

        # Knappar
        self.button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self.button_box.rejected.connect(self.reject)
        self.close_btn = self.button_box.addButton("Stäng", QDialogButtonBox.AcceptRole)
        self.close_btn.setEnabled(False)
        self.close_btn.clicked.connect(self.accept)
        layout.addWidget(self.button_box)

        self.setLayout(layout)

        self._cancelled = False

    def set_progress(self, message: str, percent: float):
        """Uppdatera progress."""
        self.status_label.setText(message)
        self.progress_bar.setValue(int(percent))

    def add_log(self, message: str):
        """Lägg till loggmeddelande."""
        self.log_text.append(message)
        # Scrolla till botten
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def set_complete(self, success: bool, message: str):
        """Markera som klar."""
        self.status_label.setText(message)
        self.progress_bar.setValue(100 if success else 0)
        self.button_box.button(QDialogButtonBox.Cancel).setEnabled(False)
        self.close_btn.setEnabled(True)

        if success:
            self.status_label.setStyleSheet("font-weight: bold; color: green;")
        else:
            self.status_label.setStyleSheet("font-weight: bold; color: red;")

    def reject(self):
        """Hantera avbryt."""
        self._cancelled = True
        super().reject()

    def is_cancelled(self) -> bool:
        """Kolla om användaren avbrutit."""
        return self._cancelled
