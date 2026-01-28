from __future__ import annotations

from PySide6.QtWidgets import QApplication

THEME_QSS = """
QWidget {
    background: #F7F9F8;
    color: #111827;
    font-family: "IBM Plex Sans", "Source Sans 3", "Noto Sans", "Segoe UI", sans-serif;
    font-size: 13px;
}

QWidget#appRoot {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #F7F9F8,
        stop:1 #FFFFFF);
}

QLabel#titleLabel {
    font-size: 20px;
    font-weight: 600;
}

QLabel#sectionLabel {
    font-size: 15px;
    font-weight: 600;
}

QLabel#helperLabel {
    color: #6B7280;
    font-size: 12px;
}

QLabel#bannerLabel {
    background: #FFF7ED;
    border: 1px solid #FDBA74;
    border-radius: 10px;
    padding: 8px 12px;
}

QFrame#card {
    background: #F8FAF9;
    border: 1px solid #E5E7EB;
    border-radius: 12px;
}

QGroupBox {
    border: 1px solid #E5E7EB;
    border-radius: 12px;
    margin-top: 10px;
    background: #FFFFFF;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 2px 6px;
    margin-left: 8px;
    font-weight: 600;
    color: #111827;
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
    min-height: 34px;
    border-radius: 10px;
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
    border-radius: 10px;
    background: #FFFFFF;
    min-height: 32px;
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
    border-radius: 8px;
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
    border-radius: 12px;
    padding: 8px;
    font-size: 13px;
}

QListWidget#navList {
    background: #FFFFFF;
    border: 1px solid #E5E7EB;
    border-radius: 14px;
    padding: 10px;
    min-width: 180px;
    font-size: 14px;
}

QListWidget#navList::item {
    padding: 10px 12px;
    border-radius: 10px;
}

QListWidget::item:selected {
    background: #E6F6F1;
    color: #111827;
}

QScrollArea {
    border: none;
    background: transparent;
}

QCheckBox {
    spacing: 8px;
}

QCheckBox::indicator {
    width: 18px;
    height: 18px;
}
"""


def apply_theme(app: QApplication) -> None:
    app.setStyleSheet(THEME_QSS)
