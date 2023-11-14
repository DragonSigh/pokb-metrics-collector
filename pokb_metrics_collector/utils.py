import os
import time
import random
import re
from datetime import date, timedelta
import pandas as pd


def retry_with_backoff(retries=5, backoff_in_seconds=1):
    """
    Декоратор для повторного запуска функции
    """

    def rwb(f):
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception:
                    if x == retries:
                        raise
                    sleep = backoff_in_seconds * 2**x + random.uniform(0, 1)
                    time.sleep(sleep)
                    x += 1

        return wrapper

    return rwb


# Функция для получения последнего файла в директории
def get_newest_file(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


def download_wait(directory, timeout, nfiles=None):
    """
    Ожидание завершения загрузки с заданным таймаутом.

    Args
    ----
    directory : str
        Путь к папке, в которую будут загружены файлы.
    timeout : int
        Сколько секунд ждать до окончания таймера.
    nfiles : int, defaults to None
        Если указано, то также ожидать ожидаемое количество файлов.
    """
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True
        for fname in files:
            if fname.endswith(".crdownload"):
                dl_wait = True
        seconds += 1
    return seconds


def save_to_excel(dframe: pd.DataFrame, path, index_arg=False):
    """
    Cохранения датафрейма в Excel с автоподбором ширины столбца
        dframe: pd.DataFrame
            датафрейм
        path:
            путь
        index_arg:
            сохранение индекса
    """
    with pd.ExcelWriter(path, mode="w", engine="openpyxl") as writer:
        dframe.to_excel(writer, index=index_arg)
        for column in dframe:
            column_width = max(dframe[column].astype(str).map(len).max(), len(column))
            col_idx = dframe.columns.get_loc(column)
            writer.sheets["Sheet1"].column_dimensions[chr(65 + col_idx)].width = column_width + 5


def get_new_department_name(x):
    """
    Обновляем старые подразделения до ОСП
    """
    value = str(x)
    if "Филиал №6 ГБУЗ МО Подольская ОКБ (Закрыто)" in value
        or 'Климовская ЦГБ' in value:
        return "ОСП 6"
    elif "ГБУЗ МО «Подольская РБ» (Закрыто)" in value:
        return "ОСП 4"
    elif 'ГБУЗ МО "Подольская городская больница №2" (Закрыто)' in value:
        return "ОСП 2"
    elif "ГБУЗ МО Львовская Районная Больница (Закрыто)" in value:
        return "ОСП 7"
    elif "ГБУЗ МО Подольская ГКБ №3 (Закрыто)" in value
        or 'ПГКБ № 3' in value:
        return "ОСП 3"
    elif (
        "ГБУЗ МО Подольская ГП №1 (Закрыто)" in value
        or 'ПГП №1' in value
        or 'Подольская городская поликлиника №1' in value
    ):
        return "ОСП 1"
    elif "ГБУЗ МО «Климовская ГБ №2» (Закрыто)" in value:
        return "ОСП 5"
    else:
        return value


def get_department(x):
    """
    Выделяем подразделения
    """
    value = str(x)
    if re.match(r"^.*ОСП №?\d.*$", value):
        return re.search(r"ОСП №?\d{1}", value)[0]
    elif re.match(r"^.*ЦАОП.*$", value):
        return "ЦАОП"
    elif re.match(r"^.*Ленинградская.*$", value):
        return "Ленинградская 9"
    elif pd.isna(x):
        return "Не указано"
    else:
        return value


# Проверить если нужный файл с отчётом за сегодняшний день уже есть в папке
def is_actual_report_exist(filepath, days=0):
    exist = os.path.isfile(filepath) and os.access(filepath, os.F_OK)
    if exist:
        created = os.path.getctime(filepath)
        if not date.fromtimestamp(created) >= date.today() - timedelta(days):
            os.remove(filepath)
            exist = False
    return exist
