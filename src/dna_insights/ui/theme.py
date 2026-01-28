from __future__ import annotations

from PySide6.QtWidgets import QApplication

THEME_QSS = """
QWidget {
    background: #FFFFFF;
    color: #111827;
    font-size: 12px;
}

QLabel#titleLabel {
    font-size: 18px;
    font-weight: 600;
}

QLabel#sectionLabel {
    font-size: 14px;
    font-weight: 600;
}

QLabel#helperLabel {
    color: #6B7280;
    font-size: 11px;
}

QLabel#bannerLabel {
    background: #FCE7F3;
    border: 1px solid #F9A8D4;
    border-radius: 8px;
    padding: 6px 10px;
}

QFrame#card {
    background: #F8FAF9;
    border: 1px solid #E5E7EB;
    border-radius: 10px;
}

QFrame#statusBanner {
    border-radius: 8px;
}

QFrame#statusBanner[kind="info"] {
    background: #F3F4F6;
    border: 1px solid #E5E7EB;
}

QFrame#statusBanner[kind="success"] {
    background: #E6F6F1;
    border: 1px solid #A7F3D0;
}

QFrame#statusBanner[kind="warning"] {
    background: #FEF3C7;
    border: 1px solid #F59E0B;
}

QFrame#statusBanner[kind="error"] {
    background: #FEE2E2;
    border: 1px solid #DC2626;
}

QPushButton {
    padding: 8px 14px;
    border-radius: 8px;
    border: 1px solid #D1D5DB;
    background: #FFFFFF;
}

QPushButton:hover {
    background: #F3F4F6;
}

QPushButton#primaryButton {
    background: #1F8A70;
    border: 1px solid #1F8A70;
    color: #FFFFFF;
    font-weight: 600;
}

QPushButton#primaryButton:hover {
    background: #18715C;
    border: 1px solid #18715C;
}

QPushButton#secondaryButton {
    background: #FFFFFF;
    border: 1px solid #E84A8A;
    color: #E84A8A;
}

QPushButton#secondaryButton:hover {
    background: #FCE7F3;
}

QPushButton#linkButton {
    background: transparent;
    border: none;
    color: #E84A8A;
    padding: 0px;
    text-align: left;
}

QPushButton:disabled {
    background: #E5E7EB;
    color: #9CA3AF;
    border-color: #E5E7EB;
}

QLineEdit,
QComboBox {
    padding: 6px 10px;
    border: 1px solid #E5E7EB;
    border-radius: 8px;
    background: #FFFFFF;
}

QLineEdit:focus,
QComboBox:focus {
    border: 1px solid #1F8A70;
}

QComboBox::drop-down {
    border: none;
}

QProgressBar {
    border: 1px solid #E5E7EB;
    border-radius: 6px;
    background: #F8FAF9;
    text-align: center;
}

QProgressBar::chunk {
    background: #1F8A70;
    border-radius: 6px;
}

QListWidget {
    background: #F8FAF9;
    border: 1px solid #E5E7EB;
    border-radius: 10px;
    padding: 6px;
}

QListWidget::item:selected {
    background: #E6F6F1;
    color: #111827;
}
"""


def apply_theme(app: QApplication) -> None:
    app.setStyleSheet(THEME_QSS)
