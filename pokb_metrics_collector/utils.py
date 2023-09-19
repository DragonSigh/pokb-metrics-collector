import os
import time
import random


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
