from __future__ import annotations

from PySide6.QtWidgets import QApplication

THEME_QSS = """
QWidget {
    background: #F9F6F2;
    color: #111827;
    font-family: "IBM Plex Sans", "Source Sans 3", "Noto Sans", "Segoe UI", sans-serif;
    font-size: 13px;
}

QWidget#appRoot {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #F9F6F2,
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
    color: #6F6A66;
    font-size: 12px;
}

QLabel#bannerLabel {
    background: #FFF3E6;
    border: 1px solid #F7C59F;
    border-radius: 10px;
    padding: 8px 12px;
}

QFrame#card {
    background: #FFFFFF;
    border: 1px solid #E9E2DA;
    border-radius: 14px;
}

QFrame#topBar {
    background: #FFFFFF;
    border: 1px solid #E9E2DA;
    border-radius: 14px;
}

QLabel#profileChip {
    background: #E6F6F1;
    border: 1px solid #B7E4D7;
    border-radius: 12px;
    padding: 4px 10px;
    font-weight: 600;
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
    background: #2B8C74;
    border: 1px solid #2B8C74;
    color: #FFFFFF;
    font-weight: 600;
}

QPushButton#primaryButton:hover {
    background: #23725E;
    border: 1px solid #23725E;
}

QPushButton#secondaryButton {
    background: #FFFFFF;
    border: 1px solid #E56F9A;
    color: #E56F9A;
}

QPushButton#secondaryButton:hover {
    background: #FCE7F3;
}

QPushButton#linkButton {
    background: transparent;
    border: none;
    color: #E56F9A;
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
    border: 1px solid #E9E2DA;
    border-radius: 10px;
    background: #FFFFFF;
    min-height: 32px;
}

QLineEdit:focus,
QComboBox:focus {
    border: 1px solid #2B8C74;
}

QComboBox::drop-down {
    border: none;
}

QProgressBar {
    border: 1px solid #E9E2DA;
    border-radius: 8px;
    background: #FFFDFB;
    text-align: center;
}

QProgressBar::chunk {
    background: #2B8C74;
    border-radius: 6px;
}

QListWidget {
    background: #FFFFFF;
    border: 1px solid #E9E2DA;
    border-radius: 14px;
    padding: 8px;
    font-size: 13px;
}

QListWidget#navList {
    background: #FFFFFF;
    border: 1px solid #E9E2DA;
    border-radius: 16px;
    padding: 10px;
    min-width: 180px;
    font-size: 14px;
}

QListWidget#navList::item {
    padding: 10px 12px;
    border-radius: 12px;
}

QListWidget::item:selected {
    background: #E6F6F1;
    color: #111827;
}

QListWidget#navList::item:selected {
    background: #E6F6F1;
    border: 1px solid #B7E4D7;
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
