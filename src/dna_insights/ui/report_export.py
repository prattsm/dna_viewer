from __future__ import annotations

from pathlib import Path

from PySide6.QtPrintSupport import QPrinter
from PySide6.QtGui import QTextDocument
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from dna_insights.app_state import AppState
from dna_insights.core.insight_engine import build_clinvar_summary
from dna_insights.core.report import build_html_report
from dna_insights.ui.widgets import prompt_passphrase


class ReportExportPage(QWidget):
    def __init__(self, state: AppState, parent=None) -> None:
        super().__init__(parent)
        self.state = state

        self.redacted_checkbox = QCheckBox("Redacted report (omit genotypes)")
        self.encrypt_checkbox = QCheckBox("Encrypt exported report")
        self.encrypt_checkbox.setEnabled(self.state.encryption.is_enabled())
        self.export_html_button = QPushButton("Export HTML")
        self.export_pdf_button = QPushButton("Export PDF")
        self.status_label = QLabel("")

        title_label = QLabel("Report Export")
        title_label.setObjectName("titleLabel")
        helper_label = QLabel("Export a report for the current profile.")
        helper_label.setObjectName("helperLabel")

        layout = QVBoxLayout()
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        layout.addWidget(title_label)
        layout.addWidget(helper_label)
        layout.addWidget(self.redacted_checkbox)
        layout.addWidget(self.encrypt_checkbox)
        layout.addWidget(self.export_html_button)
        layout.addWidget(self.export_pdf_button)
        layout.addWidget(self.status_label)
        layout.addStretch()
        self.setLayout(layout)

        self.export_html_button.clicked.connect(self._export_html)
        self.export_pdf_button.clicked.connect(self._export_pdf)
        self.state.data_changed.connect(self._sync_encryption)

        self._sync_encryption()

    def _sync_encryption(self) -> None:
        enabled = self.state.encryption.is_enabled()
        self.encrypt_checkbox.setEnabled(enabled)
        if not enabled:
            self.encrypt_checkbox.setChecked(False)

    def _ensure_profile(self):
        profile = self.state.current_profile()
        if not profile:
            QMessageBox.information(self, "Export", "Select a profile first.")
            return None, None, None
        import_info = self.state.db.get_latest_import(profile["id"])
        if not import_info:
            QMessageBox.information(self, "Export", "Import a file before exporting a report.")
            return None, None, None
        insights = self.state.db.get_latest_insights(profile["id"])
        if not insights:
            QMessageBox.information(self, "Export", "No insights available for export.")
            return None, None, None
        if self.state.settings.opt_in_categories.get("clinical", False):
            clinvar_import = self.state.db.get_latest_clinvar_import()
            if clinvar_import:
                count = self.state.db.count_clinvar_matches(profile["id"])
                sample = self.state.db.get_clinvar_matches(profile["id"], limit=3)
                insights.append(build_clinvar_summary(count, sample, clinvar_import))
        if self.redacted_checkbox.isChecked():
            for item in insights:
                item["genotypes"] = {}
        return profile, import_info, insights

    def _maybe_encrypt(self, data: bytes) -> bytes | None:
        if not (self.state.encryption.is_enabled() and self.encrypt_checkbox.isChecked()):
            return data
        if not self.state.encryption.has_key():
            passphrase = prompt_passphrase(self, confirm=False)
            if not passphrase:
                return None
            self.state.encryption.unlock(passphrase)
        return self.state.encryption.encrypt_bytes(data)

    def _export_html(self) -> None:
        profile, import_info, insights = self._ensure_profile()
        if not profile:
            return
        html = build_html_report(profile, import_info, insights, self.state.manifest.kb_version)
        file_path, _ = QFileDialog.getSaveFileName(self, "Export HTML", "report.html", "HTML (*.html)")
        if not file_path:
            return
        output = self._maybe_encrypt(html.encode("utf-8"))
        if output is None:
            QMessageBox.information(self, "Export", "Export cancelled.")
            return
        Path(file_path).write_bytes(output)
        self.status_label.setText(f"Exported to {file_path}")

    def _export_pdf(self) -> None:
        profile, import_info, insights = self._ensure_profile()
        if not profile:
            return
        html = build_html_report(profile, import_info, insights, self.state.manifest.kb_version)
        file_path, _ = QFileDialog.getSaveFileName(self, "Export PDF", "report.pdf", "PDF (*.pdf)")
        if not file_path:
            return

        document = QTextDocument()
        document.setHtml(html)
        printer = QPrinter(QPrinter.HighResolution)
        printer.setOutputFormat(QPrinter.PdfFormat)
        printer.setOutputFileName(file_path)
        document.print_(printer)

        if self.encrypt_checkbox.isChecked() and self.state.encryption.is_enabled():
            output = self._maybe_encrypt(Path(file_path).read_bytes())
            if output is None:
                QMessageBox.information(self, "Export", "Export cancelled.")
                return
            Path(file_path).write_bytes(output)
        self.status_label.setText(f"Exported to {file_path}")
