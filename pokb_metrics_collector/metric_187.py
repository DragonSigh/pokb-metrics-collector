import config
import utils
import bi_emias
import os
from datetime import date, timedelta
import pandas as pd
from loguru import logger

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date

metric_path = config.current_path + "\\reports\\Показатель 187"

pd.set_option("max_colwidth", 120)
pd.set_option("display.width", 500)


def analyze_187_data():
    # Загружаем отчёт в датафрейм
    # http://bi.mz.mosreg.ru/#form/palliativ_kr_lasttap_mm
    # Наблюдение паллиативного пациента не менее 1 раза в месяц  (показатель КР 187)
    df_pall = pd.read_excel(
        config.reports_path
        + "Наблюдение паллиативного пациента не менее 1 раза в месяц  (показатель КР 187).xlsx",
        skiprows=1,
        header=0,
    )
    # Только Подольская ОКБ
    df_pall = df_pall[(df_pall["ОГРН"] == 1215000036305)]

    df_pall["Дата последнего ТАПа"] = pd.to_datetime(
        df_pall["Дата последнего ТАПа"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date
    df_pall["Отделение"] = df_pall["Подразделение"]
    df_pall["Подразделение"] = df_pall["Подразделение"].apply(utils.get_department)

    df_pall = df_pall[
        [
            "Подразделение",
            "Отделение",
            "Фамилия пациента",
            "Имя пациента",
            "Отчество пациента",
            "Год рождения пациента",
            "Возраст пациента",
            "Дата последнего ТАПа",
        ]
    ]

    for department in df_pall["Подразделение"].unique():
        df_temp = df_pall[df_pall["Подразделение"] == department].drop(["Подразделение"], axis=1)
        df_temp = df_temp.sort_values(by="Дата последнего ТАПа")
        # Фильтрация датафрейма по уникальному значению в колонке
        styler = df_temp.style
        styler = styler.map(highlight_patients, subset=["Дата последнего ТАПа"])
        styler.to_excel(metric_path + "\\" + str(department) + ".xlsx")


def highlight_patients(s):
    background = ""
    if not isinstance(s, pd._libs.tslibs.nattype.NaTType):
        if s <= (date.today() - timedelta(days=30)):
            background = "background-color: red"
        elif (s <= (date.today() - timedelta(days=20))) and (
            s >= (date.today() - timedelta(days=29))
        ):
            background = "background-color: yellow"
    return background


analyze_187_data()
