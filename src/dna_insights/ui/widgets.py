from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QLineEdit,
    QVBoxLayout,
)


class PassphraseDialog(QDialog):
    def __init__(self, title: str, confirm: bool = False, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self._confirm = confirm

        self.passphrase_input = QLineEdit()
        self.passphrase_input.setEchoMode(QLineEdit.Password)

        self.confirm_input = QLineEdit()
        self.confirm_input.setEchoMode(QLineEdit.Password)
        self.confirm_input.setVisible(confirm)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Passphrase"))
        layout.addWidget(self.passphrase_input)
        if confirm:
            layout.addWidget(QLabel("Confirm passphrase"))
            layout.addWidget(self.confirm_input)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def passphrase(self) -> str | None:
        if self.result() != QDialog.Accepted:
            return None
        value = self.passphrase_input.text().strip()
        if not value:
            return None
        if self._confirm and value != self.confirm_input.text().strip():
            return None
        return value


def prompt_passphrase(parent=None, confirm: bool = False) -> str | None:
    title = "Set passphrase" if confirm else "Unlock encryption"
    dialog = PassphraseDialog(title=title, confirm=confirm, parent=parent)
    if dialog.exec() != QDialog.Accepted:
        return None
    return dialog.passphrase()
