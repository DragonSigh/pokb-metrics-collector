import os
import time
import random
import re
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
                    sleep = (backoff_in_seconds * 2 ** x +
                             random.uniform(0, 1))
                    time.sleep(sleep)
                    x += 1
        return wrapper
    return rwb


def complex_function(x):
    if isinstance(x, str):
        first_name = x.split(' ')[1]
        second_name = x.split(' ')[2]
        last_name = x.split(' ')[3].replace(',', '')
        return f'{first_name} {second_name} {last_name}'
    else:
        return 0


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
            if fname.endswith('.crdownload'):
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
    with pd.ExcelWriter(path, mode='w', engine='openpyxl') as writer:
        dframe.to_excel(writer, index=index_arg)
        for column in dframe:
            column_width = max(dframe[column].astype(str).map(len).max(), len(column))
            col_idx = dframe.columns.get_loc(column)
            writer.sheets['Sheet1'].column_dimensions[chr(65+col_idx)].width = column_width + 5


def get_department(x):
    """
    Выделяем подразделения
    """
    if re.match(r'ОСП \d', x):
        return re.search(r'ОСП \d', x)[0]
    elif re.match(r'ЦАОП', x):
        return 'ЦАОП'
    elif re.match(r'Ленинградская', x):
        return 'Ленинградская 9'
    else:
        return '0'
