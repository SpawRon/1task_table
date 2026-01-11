"""
набор функциональных тестов для модуля `app.py`. Тесты проверяют
импорт данных из Excel в SQLite базу: успешные сценарии, обработку
ошибок при отсутствии листов/колонок, пустых значениях и ссылках на
несуществующие скважины. Тесты используют временные файлы базы и
Excel, чтобы не модифицировать исходные данные.
"""

import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
import unittest

import pandas as pd

import app


def create_temp_db(original_db: str) -> str:
    "создаёт временную копию базы данных для теста и возвращает путь."
    temp_dir = tempfile.mkdtemp()
    temp_db_path = os.path.join(temp_dir, "database.db")
    shutil.copyfile(original_db, temp_db_path)
    return temp_db_path


class TestImportExcel(unittest.TestCase):
    "набор тестов для функции import_excel_to_db."

    def test_successful_import(self) -> None:
        "проверяет успешный импорт файла journal.xlsx в базу."
        # подготавливаем временную базу
        original_db = os.path.join(Path(__file__).resolve().parent, "database")
        temp_db = create_temp_db(original_db)

        excel_path = os.path.join(Path(__file__).resolve().parent, "journal.xlsx")
        self.assertTrue(os.path.exists(excel_path), "файл journal.xlsx должен существовать рядом с тестами")

        success, errors = app.import_excel_to_db(excel_path, temp_db)
        self.assertTrue(success, f"импорт должен завершиться успешно, но вернулись ошибки: {errors}")
        self.assertEqual(errors, [])

        # проверяем, что данные импортировались
        conn = sqlite3.connect(temp_db)
        cur = conn.cursor()
        holes_count = cur.execute("SELECT COUNT(*) FROM holes").fetchone()[0]
        assay_count = cur.execute("SELECT COUNT(*) FROM assay").fetchone()[0]
        conn.close()
        # в исходном файле journal.xlsx 5 скважин и 5 строк опробования
        self.assertGreaterEqual(holes_count, 5)
        self.assertGreaterEqual(assay_count, 5)


    def test_missing_sheet(self) -> None:
        "проверяет, что при отсутствии листа Holes возникает ошибка."
        temp_db = create_temp_db(os.path.join(Path(__file__).resolve().parent, "database"))
        # создаём временный excel без листа Holes
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            with pd.ExcelWriter(tmp.name) as writer:
                df = pd.DataFrame({"ОБЪЕКТ": ["A"], "ОТ": [0], "ДО": [1], "Au": [1.0]})
                df.to_excel(writer, sheet_name="Assay", index=False)
            excel_path = tmp.name
        success, errors = app.import_excel_to_db(excel_path, temp_db)
        self.assertFalse(success)
        self.assertTrue(any("Holes" in err for err in errors))


    def test_missing_column(self) -> None:
        "проверяет, что при отсутствии обязательной колонки возникает ошибка."
        temp_db = create_temp_db(os.path.join(Path(__file__).resolve().parent, "database"))
        # создаём excel с листом Holes, но без колонки 'X'
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            with pd.ExcelWriter(tmp.name) as writer:
                holes_df = pd.DataFrame({
                    "ИМЯ": ["A"],
                    # 'X' отсутствует
                    "Y": [1],
                    "Z": [1],
                    "ДЛИНА": [10],
                    "ГОРИЗОНТ": [100],
                    "ДАТА ПРОХОДКИ": [20231231],
                })
                holes_df.to_excel(writer, sheet_name="Holes", index=False)
                assay_df = pd.DataFrame({"ОБЪЕКТ": ["A"], "ОТ": [0], "ДО": [1], "Au": [1.0]})
                assay_df.to_excel(writer, sheet_name="Assay", index=False)
            excel_path = tmp.name
        success, errors = app.import_excel_to_db(excel_path, temp_db)
        self.assertFalse(success)
        self.assertTrue(any("Колонка 'X'" in err for err in errors))


    def test_missing_values(self) -> None:
        "проверяет обработку пустых значений."
        temp_db = create_temp_db(os.path.join(Path(__file__).resolve().parent, "database"))
        # Создаём excel с NaN в колонке 'Y'
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            with pd.ExcelWriter(tmp.name) as writer:
                holes_df = pd.DataFrame({
                    "ИМЯ": ["A"],
                    "X": [0],
                    "Y": [pd.NA],  # Пустое значение
                    "Z": [1],
                    "ДЛИНА": [10],
                    "ГОРИЗОНТ": [100],
                    "ДАТА ПРОХОДКИ": [20231231],
                })
                holes_df.to_excel(writer, sheet_name="Holes", index=False)
                assay_df = pd.DataFrame({"ОБЪЕКТ": ["A"], "ОТ": [0], "ДО": [1], "Au": [1.0]})
                assay_df.to_excel(writer, sheet_name="Assay", index=False)
            excel_path = tmp.name
        success, errors = app.import_excel_to_db(excel_path, temp_db)
        self.assertFalse(success)
        self.assertTrue(any("пустые значения" in err.lower() or "пустые" in err.lower() for err in errors))


    def test_unknown_hole_in_assay(self) -> None:
        "проверяет, что импорт не ломает базу при ошибке."
        temp_db = create_temp_db(os.path.join(Path(__file__).resolve().parent, "database"))
        
        # 1. Запоминаем, сколько строк в базе БЫЛО изначально
        conn = sqlite3.connect(temp_db)
        initial_count = conn.execute("SELECT COUNT(*) FROM assay").fetchone()[0]
        conn.close()

        # 2. Создаем Excel с ошибкой (скважина B не существует)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            with pd.ExcelWriter(tmp.name) as writer:
                pd.DataFrame({"ИМЯ": ["A"], 
                                "X":[0], 
                                "Y":[0], 
                                "Z":[0], 
                                "ДЛИНА":[10], 
                                "ГОРИЗОНТ":[100], 
                                "ДАТА ПРОХОДКИ":[20231231]}).to_excel(writer, sheet_name="Holes", index=False)
                pd.DataFrame({"ОБЪЕКТ": ["B"], 
                                "ОТ": [0], 
                                "ДО": [1], 
                                "Au": [1.0]}).to_excel(writer, sheet_name="Assay", index=False)
            excel_path = tmp.name

        # 3. Пытаемся импортировать
        success, errors = app.import_excel_to_db(excel_path, temp_db)
        self.assertFalse(success) # Должна быть ошибка

        # 4. Проверяем, что в базе осталось столько же строк, сколько было (rollback сработал)
        conn = sqlite3.connect(temp_db)
        current_count = conn.execute("SELECT COUNT(*) FROM assay").fetchone()[0]
        conn.close()
        
        # Сравниваем с initial_count, а не с нулем!
        self.assertEqual(current_count, initial_count, "база должна была откатиться к исходному состоянию")
if __name__ == "__main__":
    unittest.main()
