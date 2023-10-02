import config
import utils
import bi_emias
import os
import json
import pandas as pd
from loguru import logger

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date

metric_path = config.current_path + "\\reports\\Показатель 55"


# @utils.retry_with_backoff(retries=5)
def start_053_report_saving():
    # Получить путь к файлу с данными для авторизации
    credentials_path = os.path.join(config.current_path, "auth-bi-emias.json")
    f = open(credentials_path, "r", encoding="utf-8")
    data = json.load(f)
    f.close()
    for _departments in data["departments"]:
        for _units in _departments["units"]:
            bi_emias.authorize(_units["login"], _units["password"])
    if not utils.is_actual_report_exist(
        config.reports_path
        + "\\Мониторинг выезда скорой помощи к пациентам, состоящих на д-учете.xlsx"
    ):
        bi_emias.load_any_report("smp_disp", first_date, last_date)
        bi_emias.export_report()
    if not utils.is_actual_report_exist(
        config.reports_path + "\\Детализация по картам диспансерного наблюдения.xlsx", 14
    ):
        bi_emias.load_any_report("dispensary_patients", first_date, last_date)
        bi_emias.export_report()

    logger.debug("Выгрузка из BI ЕМИАС завершена")


def analyze_053_data():
    # Загружаем отчёты в датафрейм
    # 1.1 http://bi.mz.mosreg.ru/#form/smp_disp
    # Мониторинг выезда СМП к пациентам, состоящих на ДУ
    smp_disp = pd.read_excel(
        config.reports_path
        + "\\Мониторинг выезда скорой помощи к пациентам, состоящих на д-учете.xlsx",
        skiprows=1,
        header=0,
    )
    # 1.2 http://bi.mz.mosreg.ru/#form/dispensary_patients
    # Детализация по картам диспансерного наблюдения
    # Можно выгружать раз в 2-3 недели
    dispensary_patients = pd.read_excel(
        config.reports_path + "\\Детализация по картам диспансерного наблюдения.xlsx",
        skiprows=1,
        header=0,
    )


start_053_report_saving()
analyze_053_data()
