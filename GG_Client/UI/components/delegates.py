from PyQt5.QtWidgets import QStyledItemDelegate, QComboBox
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class ReadOnlyItemDelegate(QStyledItemDelegate):
    def __init__(self, editable_columns, parent=None):
        super().__init__(parent)
        self.editable_columns = editable_columns

    def paint(self, painter, option, index):
        # 편집할 수 없는 컬럼인 경우 배경색을 녹색으로 설정
        if index.column() not in self.editable_columns:
            option.backgroundBrush = QColor(0, 255, 0)  # 녹색
        super().paint(painter, option, index)


class ComboBoxDelegate(QStyledItemDelegate):
    def __init__(self, options_dict=None, parent=None):
        super(ComboBoxDelegate, self).__init__(parent)
        self.options_dict = (
            options_dict or {}
        )  # {(row, column): [(display, value), ...]}

    def createEditor(self, parent, option, index):
        key = (index.row(), index.column())
        if key in self.options_dict:
            editor = QComboBox(parent)
            for display_text, value in self.options_dict[key]:
                editor.addItem(display_text, value)  # 👈 display와 value 분리
            return editor
        return super().createEditor(parent, option, index)

    def setEditorData(self, editor, index):
        if isinstance(editor, QComboBox):
            value = index.model().data(index, Qt.EditRole)
            idx = editor.findData(value)  # 👈 value로 검색
            if idx >= 0:
                editor.setCurrentIndex(idx)
        else:
            super().setEditorData(editor, index)

    def setModelData(self, editor, model, index):
        if isinstance(editor, QComboBox):
            value = editor.currentData()  # 👈 실제 value 저장
            model.setData(index, value, Qt.EditRole)
        else:
            super().setModelData(editor, model, index)
