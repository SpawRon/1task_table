"""
Приложение и вспомогательные функции для импорта данных из Excel‑журнала
в SQLite базу данных и отображения результатов. В рамках тестового
задания интерфейс строится на PyQt5 (при отсутствии библиотеки код
можно адаптировать под PySide или запустить только импорт как
скрипт). В разделе `if __name__ == "__main__"` показан пример
инициализации и запуска виджета.

Функция `import_excel_to_db()` вынесена отдельно для возможности
тестирования без графического интерфейса. Она читает Excel, проверяет
наличие обязательных листов и колонок, отсутствие пустых значений,
вставляет строки в таблицы `holes` и `assay` (обновляя имеющиеся
скважины по имени) и возвращает кортеж `(успех, список_ошибок)`.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Dict, List, Tuple

import pandas as pd

# Попытка загрузить PyQt5. Если библиотеки нет (например, в среде
# тестирования), импорт происходит в блоке `try` для того, чтобы
# модуль оставался импортируемым. В таком случае GUI‑часть будет
# недоступна, но функцию `import_excel_to_db()` можно тестировать.

from PyQt5 import QtWidgets, QtCore, QtGui

# Блок TYPE_CHECKING теперь не нужен для работы, 
# так как библиотеки импортированы напрямую.



def import_excel_to_db(excel_path: str, db_path: str) -> Tuple[bool, List[str]]:
    """Импортирует данные из Excel‑файла в SQLite базу данных.

    Аргументы:
        excel_path: путь к XLSX файлу с листами "Holes" и "Assay".
        db_path: путь к SQLite базе данных с таблицами `holes` и `assay`.

    Возвращает:
        (успех, список_ошибок). В случае успешного импорта `успех` равен
        True и список ошибок пуст. При возникновении ошибок `успех` равен
        False, и список содержит сообщения об ошибках. В случае ошибки
        транзакция откатывается, база не модифицируется.
    """
    errors = []
    try:
        # Конструкция 'with' гарантирует закрытие файла после чтения
        with pd.ExcelFile(excel_path) as xls:
            required_sheets = ["Holes", "Assay"]
            for sheet in required_sheets:
                if sheet not in xls.sheet_names:
                    errors.append(f"Отсутствует лист '{sheet}' в Excel")
            if errors: return False, errors

            holes_df = xls.parse("Holes")
            assay_df = xls.parse("Assay")
            holes_df.rename(columns=lambda c: str(c).strip(), inplace=True)
            assay_df.rename(columns=lambda c: str(c).strip(), inplace=True)
    except Exception as e:
        return False, [f"Ошибка чтения или разбора Excel: {e}"]
    errors: List[str] = []
    # Проверка существования файлов
    if not os.path.isfile(excel_path):
        return False, [f"Файл Excel не найден: {excel_path}"]
    if not os.path.isfile(db_path):
        return False, [f"Файл базы данных не найден: {db_path}"]
    try:
        # Загружаем книгу
        xls = pd.ExcelFile(excel_path)
    except Exception as e:
        return False, [f"Ошибка чтения Excel файла: {e}"]

    # Проверяем наличие обязательных листов
    required_sheets = ["Holes", "Assay"]
    for sheet in required_sheets:
        if sheet not in xls.sheet_names:
            errors.append(f"Отсутствует лист '{sheet}' в Excel")
    if errors:
        return False, errors

    # Загружаем данные
    try:
        holes_df = xls.parse("Holes")
        assay_df = xls.parse("Assay")
        # Удаляем возможные пробелы в именах колонок
        holes_df.rename(columns=lambda c: str(c).strip(), inplace=True)
        assay_df.rename(columns=lambda c: str(c).strip(), inplace=True)
    except Exception as e:
        return False, [f"Ошибка разбора листов: {e}"]

    # Ожидаемые колонки и их соответствие полям в БД
    holes_cols: Dict[str, str] = {
        "ИМЯ": "name",
        "X": "x",
        "Y": "y",
        "Z": "z",
        "ДЛИНА": "lenght",
        "ГОРИЗОНТ": "_level",
        "ДАТА ПРОХОДКИ": "issue_date",
    }
    assay_cols: Dict[str, str] = {
        "ОБЪЕКТ": "hole_name",
        "ОТ": "_from",
        "ДО": "_to",
        "Au": "Au",
    }

    # Проверяем наличие колонок
    for col in holes_cols:
        if col not in holes_df.columns:
            errors.append(f"Колонка '{col}' отсутствует на листе Holes")
    for col in assay_cols:
        if col not in assay_df.columns:
            errors.append(f"Колонка '{col}' отсутствует на листе Assay")
    if errors:
        return False, errors

    # Проверяем пустые значения
    if holes_df[list(holes_cols.keys())].isnull().any().any():
        errors.append("В листе Holes обнаружены пустые значения. Пожалуйста, заполните все поля.")
    if assay_df[list(assay_cols.keys())].isnull().any().any():
        errors.append("В листе Assay обнаружены пустые значения. Пожалуйста, заполните все поля.")
    if errors:
        return False, errors

    # Подключаемся к базе и начинаем транзакцию
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Включаем принудительное использование внешних ключей (на всякий случай)
        cur.execute("PRAGMA foreign_keys = ON;")

        # Создаём отображение имя скважины -> id (если в базе уже есть)
        hole_name_to_id: Dict[str, int] = {}
        cur.execute("SELECT id, name FROM holes")
        for row in cur.fetchall():
            hole_name_to_id[row[1]] = row[0]

        # Обрабатываем скважины. Если скважина с таким именем уже есть в БД,
        # мы не дублируем её, но можем обновить координаты/другие поля.
        for idx, row in holes_df.iterrows():
            name = row["ИМЯ"]
            x_val = row["X"]
            y_val = row["Y"]
            z_val = row["Z"]
            length_val = row["ДЛИНА"]
            level_val = row["ГОРИЗОНТ"]
            issue_date_val = row["ДАТА ПРОХОДКИ"]

            if name in hole_name_to_id:
                # Обновляем существующую запись, чтобы данные оставались актуальными
                hole_id = hole_name_to_id[name]
                cur.execute(
                    "UPDATE holes SET x = ?, y = ?, z = ?, lenght = ?, _level = ?, issue_date = ? WHERE id = ?",
                    (x_val, y_val, z_val, length_val, level_val, issue_date_val, hole_id),
                )
            else:
                # Вставляем новую запись
                cur.execute(
                    "INSERT INTO holes (name, x, y, z, lenght, _level, issue_date) VALUES (?,?,?,?,?,?,?)",
                    (name, x_val, y_val, z_val, length_val, level_val, issue_date_val),
                )
                hole_id = cur.lastrowid
                hole_name_to_id[name] = hole_id

        # Обрабатываем опробование (assay)
        for idx, row in assay_df.iterrows():
            hole_name = row["ОБЪЕКТ"]
            from_val = row["ОТ"]
            to_val = row["ДО"]
            au_val = row["Au"]

            hole_id = hole_name_to_id.get(hole_name)
            if hole_id is None:
                # Скважины может не быть, это ошибка
                errors.append(f"Скважина '{hole_name}' отсутствует в листе Holes, строка {idx + 2}")
                continue
            # Вставляем запись опробования
            cur.execute(
                "INSERT INTO assay (hole_id, _from, _to, Au) VALUES (?,?,?,?)",
                (hole_id, from_val, to_val, au_val),
            )

        if errors:
            # При ошибках откатываем изменения
            conn.rollback()
            conn.close()
            return False, errors

        conn.commit()
        conn.close()
        return True, []
    except Exception as e:
        # При исключениях также откатываем изменения
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False, [f"Ошибка базы данных: {e}"]


if QtWidgets is not None:
    class ImportWidget(QtWidgets.QWidget):
        """виджет для импорта Excel‑файлов в SQLite и просмотра данных."""

        def __init__(self, db_path: str, parent = None) -> None:
            super().__init__(parent)
            self.db_path = db_path
            self.setup_ui()

        def setup_ui(self) -> None:
            self.setWindowTitle("Импорт журнала в базу данных")

            root = QtWidgets.QVBoxLayout(self)

            # ===== Верхний блок (как на схеме) =====
            top_box = QtWidgets.QGroupBox(self)
            top_box.setTitle("")                      # без заголовка
            top_box.setFlat(False)
            top_layout = QtWidgets.QGridLayout(top_box)
            top_layout.setContentsMargins(12, 12, 12, 12)
            top_layout.setHorizontalSpacing(10)
            top_layout.setVerticalSpacing(10)

            # input1 + btn1 (в одной строке)
            self.file_line_edit = QtWidgets.QLineEdit(top_box)
            self.file_line_edit.setPlaceholderText("Выберите Excel файл...")

            btn_browse = QtWidgets.QPushButton("Обзор", top_box)
            btn_browse.setFixedWidth(110)             # чтобы был компактным как на схеме
            btn_browse.clicked.connect(self.browse_file)

            top_layout.addWidget(self.file_line_edit, 0, 0, 1, 1)
            top_layout.addWidget(btn_browse,          0, 1, 1, 1, alignment=QtCore.Qt.AlignRight)

            # btn2 справа ниже
            self.btn_import = QtWidgets.QPushButton("Импортировать", top_box)
            self.btn_import.setFixedWidth(160)
            self.btn_import.clicked.connect(self.handle_import)

            top_layout.addWidget(self.btn_import, 1, 1, alignment=QtCore.Qt.AlignRight)

            # растяжки, чтобы btn2 был "в середине блока", как на макете
            top_layout.setRowStretch(0, 0)
            top_layout.setRowStretch(1, 1)  # “пустота” под первой строкой
            top_layout.setColumnStretch(0, 1)  # input растягивается
            top_layout.setColumnStretch(1, 0)  # кнопки фикс.ширины

            root.addWidget(top_box, stretch=0)

            # ===== Нижний блок =====
            bottom_box = QtWidgets.QGroupBox(self)
            bottom_box.setTitle("")
            bottom_layout = QtWidgets.QGridLayout(bottom_box)
            bottom_layout.setContentsMargins(12, 12, 12, 12)
            bottom_layout.setHorizontalSpacing(10)
            bottom_layout.setVerticalSpacing(10)

            # btn3 справа сверху
            self.btn_show = QtWidgets.QPushButton("Показать данные", bottom_box)
            self.btn_show.setFixedWidth(160)
            self.btn_show.clicked.connect(self.show_data)
            bottom_layout.addWidget(self.btn_show, 0, 1, alignment=QtCore.Qt.AlignRight)

            # table1 ниже на всю ширину
            self.table = QtWidgets.QTableWidget(bottom_box)
            self.table.setColumnCount(4)
            self.table.setHorizontalHeaderLabels(["Имя скважины", "ОТ", "ДО", "Au"])
            bottom_layout.addWidget(self.table, 1, 0, 1, 2)

            bottom_layout.setColumnStretch(0, 1)
            bottom_layout.setColumnStretch(1, 0)
            bottom_layout.setRowStretch(1, 1)

            root.addWidget(bottom_box, stretch=1)


        def browse_file(self) -> None:
            """Открывает диалог выбора файла и отображает путь."""
            options = QtWidgets.QFileDialog.Options()
            file_name, _ = QtWidgets.QFileDialog.getOpenFileName(
                self,
                "Выберите Excel файл",
                "",
                "Excel Files (*.xlsx *.xls);;All Files (*)",
                options=options,
            )
            if file_name:
                self.file_line_edit.setText(file_name)

        def handle_import(self) -> None:
            """Запускает импорт выбранного файла."""
            excel_path = self.file_line_edit.text().strip()
            if not excel_path:
                QtWidgets.QMessageBox.warning(self, "Внимание", "Пожалуйста, выберите Excel файл.")
                return
            success, messages = import_excel_to_db(excel_path, self.db_path)
            if success:
                QtWidgets.QMessageBox.information(self, "Успех", "Данные успешно импортированы.")
            else:
                QtWidgets.QMessageBox.critical(self, "Ошибка", "\n".join(messages))

        def show_data(self) -> None:
            """Загружает данные из БД и отображает в таблице."""
            try:
                conn = sqlite3.connect(self.db_path)
                cur = conn.cursor()
                # Делаем JOIN, чтобы получить имя скважины и опробование
                query = (
                    "SELECT h.name, a._from, a._to, a.Au "
                    "FROM assay a JOIN holes h ON a.hole_id = h.id "
                    "ORDER BY h.name, a._from"
                )
                rows = cur.execute(query).fetchall()
                conn.close()
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные: {e}")
                return
            # Заполняем таблицу
            self.table.setRowCount(len(rows))
            for row_idx, (name, from_val, to_val, au_val) in enumerate(rows):
                self.table.setItem(row_idx, 0, QtWidgets.QTableWidgetItem(str(name)))
                self.table.setItem(row_idx, 1, QtWidgets.QTableWidgetItem(str(from_val)))
                self.table.setItem(row_idx, 2, QtWidgets.QTableWidgetItem(str(to_val)))
                self.table.setItem(row_idx, 3, QtWidgets.QTableWidgetItem(str(au_val)))
            self.table.resizeColumnsToContents()


def main() -> None:
    """Точка входа для запуска приложения вручную."""
    if QtWidgets is None:
        raise SystemExit(
            "PyQt5 не установлена в текущей среде. Для запуска GUI установите пакет PyQt5 или PySide6."
        )
    import argparse

    parser = argparse.ArgumentParser(description="Импорт Excel в SQLite и просмотр данных.")
    parser.add_argument(
        "--db",
        dest="db_path",
        default="database",
        help="Путь к SQLite базе данных (по умолчанию 'database' в текущей директории)",
    )
    args = parser.parse_args()
    app = QtWidgets.QApplication([])
    widget = ImportWidget(args.db_path)
    widget.resize(800, 600)
    widget.show()
    app.exec_()


if __name__ == "__main__":
    # Если модуль запускается напрямую, стартуем приложение
    main()