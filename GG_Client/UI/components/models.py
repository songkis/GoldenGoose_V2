import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, Qt, QModelIndex

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class PandasModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), editable_columns=None, parent=None):
        """
        :param df: 표시할 pandas DataFrame
        :param editable_columns: 편집 가능한 컬럼 리스트 (예: ['USER_ID', 'PWD'])
        """
        super(PandasModel, self).__init__(parent)
        self._data = df
        self.editable_columns = (
            editable_columns if editable_columns else []
        )  # 편집 가능 컬럼 설정

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole or role == Qt.EditRole:
            return str(self._data.iloc[index.row(), index.column()])
        return None

    def setData(self, index, value, role=Qt.EditRole):
        """특정 컬럼만 편집 가능하도록 설정"""
        if index.isValid() and role == Qt.EditRole:
            column_name = self._data.columns[index.column()]
            if column_name in self.editable_columns:  # 특정 컬럼만 편집 가능
                self._data.iloc[index.row(), index.column()] = value
                self.dataChanged.emit(index, index, (Qt.DisplayRole, Qt.EditRole))
                return True
        return False

    def flags(self, index):
        """특정 컬럼만 편집 가능하도록 설정"""
        if not index.isValid():
            return Qt.NoItemFlags
        column_name = self._data.columns[index.column()]
        if column_name in self.editable_columns:
            return (
                Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsEditable
            )  # 편집 가능
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled  # 편집 불가능

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self._data.columns[section]
            elif orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None

    def update(self, df):
        """데이터프레임을 업데이트하는 메서드 추가"""
        self.beginResetModel()
        self._data = df
        self.endResetModel()


class RealDataTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self.realdata = {}
        self.headers = [
            "종목코드",
            "현재가",
            "전일대비",
            "등락률",
            "매도호가",
            "매수호가",
            "누적거래량",
            "시가",
            "고가",
            "저가",
            "거래회전율",
            "시가총액",
        ]

    def rowCount(self, index=QModelIndex()):
        return len(self.realdata)

    def columnCount(self, index=QModelIndex()):
        return len(self.headers)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self.realdata)):
            return None

        if role == Qt.DisplayRole:
            rows = []
            for k in self.realdata.keys():
                rows.append(k)
            one_row = rows[index.row()]
            selected_row = self.realdata[one_row]

            return selected_row[index.column()]

        return None

    def headerData(self, column, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self.headers[column]
        return int(column + 1)

    def flags(self, index):
        return Qt.ItemIsEnabled

    def reset(self):
        self.beginResetModel()
        self.endResetModel()
