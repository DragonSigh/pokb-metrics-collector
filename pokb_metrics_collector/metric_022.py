import config
import utils
import emias
import kornet
import shutil
import os
import json
import pandas as pd
from loguru import logger


@utils.retry_with_backoff(retries=5)
def start_emias_report_saving():
    # Очистить предыдущие результаты
    shutil.rmtree(config.reports_path, ignore_errors=True)
    # Получить путь к файлу с данными для авторизации
    credentials_path = os.path.join(config.current_path, 'auth-emias.json')
    # Определение дат
    first_date = config.first_date
    last_date = config.last_date
    # Открываем данные для авторизации и проходим по списку кабинетов
    logger.debug(f'Выбран период: с {first_date.strftime("%d.%m.%Y")} \
                  по {last_date.strftime("%d.%m.%Y")}')
    f = open(credentials_path, 'r', encoding='utf-8')
    data = json.load(f)
    f.close()
    for _departments in data['departments']:
        logger.debug(f'Начинается сохранение отчёта для подразденения: \
                      {_departments["department"]}')
        for _units in _departments["units"]:
            logger.debug(f'Начинается авторизация в отделение: \
                          {_units["name"]}')
            emias.authorize(_units['login'], _units['password'])
    # ID кабинетов выписки лекарств
    cabinets_list = ['2434', '2460', '2459', '2450', '636',
                     '2458', '2343', '2457', '2449', '2711']
    for cabinet in cabinets_list:
        emias.load_system_report(cabinet, first_date, last_date)
        emias.export_report(cabinet)
    logger.debug('Выгрузка из ЕМИАС завершена')


@utils.retry_with_backoff(retries=5)
def start_kornet_report_saving():
    # Очистить предыдущие результаты
    shutil.rmtree(config.reports_path, ignore_errors=True)
    # Получить путь к файлу с данными для авторизации
    credentials_path = os.path.join(config.current_path, 'auth-kornet.json')
    # Определение дат
    first_date = config.first_date
    last_date = config.last_date
    logger.debug(f'Выбран период: с {first_date.strftime("%d.%m.%Y")} '
                 f'по {last_date.strftime("%d.%m.%Y")}')
    f = open(credentials_path, 'r', encoding='utf-8')
    data = json.load(f)
    f.close()
    for _departments in data['departments']:
        df_list = []
        logger.debug(f'Начинается сохранение отчёта для подразденения: '
                     f'{_departments["department"]}')
        for _units in _departments["units"]:
            logger.debug('Начинается авторизация в отделение: '
                         f'{_units["name"]}')
            kornet.authorize(_units['login'], _units['password'])
            kornet.load_dlo_report(first_date, last_date)
            kornet.export_report()
            df_temp = pd.read_excel(os.path.join(config.reports_path, 'ReestrDLO.xlsx'),
                                    skiprows=range(1, 12), skipfooter=18,
                                    usecols='C,D,E,I,L,N,P,S,X')
            df_temp.insert(0, 'Отделение', _units['name'])
            df_list.append(df_temp)
            os.remove(os.path.join(config.reports_path, 'ReestrDLO.xlsx'))
        final_df = pd.concat(df_list)
        final_df.columns = ['Отделение', 'Серия и номер', 'Дата выписки',
                            'ФИО врача', 'СНИЛС', 'ФИО пациента',
                            'Код категории', 'Адрес', 'Препарат',
                            'Количество']
        final_df.to_excel(os.path.join(config.reports_path,
                          _departments['department'] + '.xlsx'),
                          index=False)
    logger.debug('Выгрузка из КОРНЕТА завершена')


tasks = [
    start_kornet_report_saving(),
    start_emias_report_saving()
]
