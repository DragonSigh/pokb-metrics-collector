import config
import utils
import bi_emias
import os
import re
import json
import pandas as pd
from loguru import logger
from datetime import date

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date

metric_path = config.current_path + "\\reports\\Показатель 24"


@utils.retry_with_backoff(retries=5)
def start_bi_report_saving():
    # Получить путь к файлу с данными для авторизации
    credentials_path = os.path.join(config.current_path, "auth-bi-emias.json")
    f = open(credentials_path, "r", encoding="utf-8")
    data = json.load(f)
    f.close()
    for _departments in data["departments"]:
        for _units in _departments["units"]:
            bi_emias.authorize(_units["login"], _units["password"])
    # Пропустить выгрузку, если нужные файлы за сегодняшний день уже есть в папке
    file1 = config.reports_path + "\\Прохождение пациентами ДВН или ПМО.xlsx"
    file2 = config.reports_path + "\\Количество карт ДВН и УДВН закрытых через ТМК.xlsx"
    created1 = os.path.getctime(file1)
    created2 = os.path.getctime(file2)

    if os.path.isfile(file1):
        if not date.fromtimestamp(created1) == date.today():
            os.remove(file1)
            bi_emias.load_any_report("pass_dvn", first_date, last_date)
            bi_emias.export_report()
    if os.path.isfile(file2):
        if not date.fromtimestamp(created2) == date.today():
            os.remove(file2)
            bi_emias.load_any_report("disp_tmk", first_date, last_date)
            bi_emias.export_report()
    logger.debug("Выгрузка из BI ЕМИАС завершена")


def analyze_data():
    #  1. Выгружаем отчет http://bi.mz.mosreg.ru/#form/disp_tmk за контролируемый период (с начала
    # текущей недели или для понедельника - вся прошлая неделя).
    df_disp_tmk = pd.read_excel(
        config.reports_path + "\\Количество карт ДВН и УДВН закрытых через ТМК.xlsx",
        skiprows=1,
        header=0
    )
    # Выбираем ПОКБ
    df_disp_tmk = df_disp_tmk.loc[df_disp_tmk["ОГРН медицинской организации"] == 1215000036305]
    # Сконвертировать время закрытия карты в дату
    df_disp_tmk["Закрытие диспансеризации через телемедицинские консультации"] = pd.to_datetime(
        df_disp_tmk["Закрытие диспансеризации через телемедицинские консультации"],
        format="%d.%m.%Y %H:%M:%S",
    ).dt.date
    # Оставляем одну фамилию
    df_disp_tmk["ФИО пациента"] = df_disp_tmk["ФИО пациента"].apply(lambda x: x.split(" ")[0])

    # 2. Выгружаем отчет http://bi.mz.mosreg.ru/#form/pass_dvn за период "Текущая дата минус месяц"
    df_pass_dvn = pd.read_excel(
        config.reports_path + "\\Прохождение пациентами ДВН или ПМО.xlsx", skiprows=1, header=0
    )

    # Сконвертировать время закрытия карты в дату
    df_pass_dvn["Дата закрытия карты диспансеризации"] = pd.to_datetime(
        df_pass_dvn["Дата закрытия карты диспансеризации"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date

    # 2.1 Причина закрытия - Обследование пройдено
    # 2.2 Дата закрытия - контролируемый период
    # 2.3 Вид обследования - 404 Диспансеризация и 404 Профилактические медицинские осмотры
    df_pass_dvn = df_pass_dvn[
        (df_pass_dvn["Причина закрытия"] == "Обследование пройдено")
        & (df_pass_dvn["Дата закрытия карты диспансеризации"] >= first_date)
        & (df_pass_dvn["Дата закрытия карты диспансеризации"] <= last_date)
    ]

    df_pass_dvn = df_pass_dvn[
        (df_pass_dvn["Вид обследования"] == "404н Диспансеризация")
        | (df_pass_dvn["Вид обследования"] == "404н Профилактические медицинские осмотры")
    ]

    # 3. Из (2) убираем записи, где "Результат обращения" содержит "Направлен на II этап"
    df_disp_tmk = df_disp_tmk.rename(
        columns={
            "Закрытие диспансеризации через телемедицинские консультации": "Дата закрытия"
        }
    )
    df_pass_dvn = df_pass_dvn.rename(
        columns={
            "Дата закрытия карты диспансеризации": "Дата закрытия",
            "Врач подписывающий заключение диспансеризации": "Врач",
        }
    )

    df_pass_dvn["Подразделение"] = df_pass_dvn["Структурное подразделение"].apply(
        lambda x: re.search(r"ОСП \d", x)[0] if re.match(r"^ОСП \d.*$", x) else "Ленинградская 9"
    )
    df_pass_dvn = df_pass_dvn.rename(columns={"Структурное подразделение": "Отделение"})

    # 4. К (2) подгружаем (1) по ключу "Фамилия + Дата закрытия карты"
    df_final = (
        df_pass_dvn.merge(
            df_disp_tmk,
            left_on=["ФИО пациента", "Дата закрытия"],
            right_on=["ФИО пациента", "Дата закрытия"],
            how="left",
            indicator=True,
        )
        .query('_merge == "left_only"')
        .drop(
            [
                "_merge",
                "#_x",
                "#_y",
                "Медицинская организация диспансеризации",
                "ОГРН",
                "ID подразделения_x",
                "ID подразделения_y",
                "Причина закрытия",
                "Процент прохождения",
                "Вид обследования",
                "Статус актуальный",
                "Дата обновления статуса",
                "Текст сообщения",
                "Группа здоровья",
                "Результат обращения",
                "Период",
                "Наименование медицинской организации",
                "ОГРН медицинской организации",
                "Дата последнего мероприятия 1 этапа диспансеризации",
                "Дата рождения пациента",
                "Дата создания карты диспансеризации",
            ],
            axis=1,
        )
    )

    try:
        os.mkdir(metric_path)
    except FileExistsError:
        pass

    for department in df_final["Подразделение"].unique():
        df_temp = (
            df_final[df_final["Подразделение"] == department]
            .drop(["Подразделение"], axis=1)
            .sort_values(["Отделение", "Врач"])
        )
        # Фильтрация датафрейма по уникальному значению в колонке
        utils.save_to_excel(df_temp, metric_path + "\\" + department + ".xlsx")

    # Аггрегация для дашборда

    df_agg = (
        df_pass_dvn.merge(
            df_disp_tmk,
            left_on=["ФИО пациента", "Дата закрытия"],
            right_on=["ФИО пациента", "Дата закрытия"],
            how="outer",
            indicator=True,
        )
        .drop(
            [
                "#_x",
                "#_y",
                "Медицинская организация диспансеризации",
                "ОГРН",
                "ID подразделения_x",
                "ID подразделения_y",
                "Причина закрытия",
                "Процент прохождения",
                "Вид обследования",
                "Статус актуальный",
                "Дата обновления статуса",
                "Текст сообщения",
                "Группа здоровья",
                "Результат обращения",
                "Период",
                "Наименование медицинской организации",
                "ОГРН медицинской организации",
                "Дата последнего мероприятия 1 этапа диспансеризации",
                "Дата рождения пациента",
                "Дата создания карты диспансеризации",
            ],
            axis=1,
        )
        .assign(count=lambda x: ~x["ФИО пациента"].isna())
        .groupby(["Подразделение", "_merge"])
        .count()
        .reset_index()
        .drop(
            ["Отделение", "Врач", "ФИО пациента", "Номер МКАБ", "Дата закрытия"],
            axis=1,
        )
    )

    df_agg["sum"] = df_agg["count"].rolling(3).sum()
    df_agg = df_agg.query('_merge == "both"')
    df_agg.loc["ПОКБ"] = df_agg.sum(numeric_only=True)
    df_agg.loc["ПОКБ", ["Подразделение"]] = "ПОКБ"
    df_agg["% по показателю 24"] = round(df_agg["count"] / df_agg["sum"] * 100).astype(int)
    df_agg = df_agg.drop(["_merge", "count", "sum"], axis=1)
    print(df_agg)
    utils.save_to_excel(df_agg, metric_path + '\\agg_24.xlsx')


start_bi_report_saving()
analyze_data()
