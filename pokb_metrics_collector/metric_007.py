import config
import utils
import bi_emias
import os
import re
import json
import pandas as pd
from loguru import logger

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date

metric_path = config.current_path + "\\reports\\Показатель 7"


@utils.retry_with_backoff(retries=5)
def start_bi_report_saving():
    if not utils.is_actual_report_exist(
        config.reports_path + "Прохождение пациентами ДВН или ПМО.xlsx"
    ):
        # Получить путь к файлу с данными для авторизации
        credentials_path = os.path.join(config.current_path, "auth-bi-emias.json")
        f = open(credentials_path, "r", encoding="utf-8")
        data = json.load(f)
        f.close()
        for _departments in data["departments"]:
            for _units in _departments["units"]:
                bi_emias.authorize(_units["login"], _units["password"])
        # Выгрузка отчета
        bi_emias.load_any_report("pass_dvn", first_date, last_date)
        bi_emias.export_report()
        logger.debug("Выгрузка из BI ЕМИАС завершена")


def analyze_data():
    # Загружаем отчёт в датафрейм
    df_pass_dvn = pd.read_excel(
        config.reports_path + "\\Прохождение пациентами ДВН или ПМО.xlsx", skiprows=1, header=0
    )

    # Сконвертировать время закрытия карты в дату
    df_pass_dvn["День недели"] = pd.to_datetime(
        df_pass_dvn["Дата закрытия карты диспансеризации"], format="%d.%m.%Y %H:%M:%S"
    ).dt.day_name("Russian")

    df_pass_dvn["День недели"] = pd.Categorical(
        df_pass_dvn["День недели"],
        categories=[
            "Понедельник",
            "Вторник",
            "Среда",
            "Четверг",
            "Пятница",
            "Суббота",
            "Воскресенье",
        ],
        ordered=True,
    )

    df_pass_dvn["Дата закрытия карты диспансеризации"] = pd.to_datetime(
        df_pass_dvn["Дата закрытия карты диспансеризации"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date

    # Только Подольская ОКБ
    df_pass_dvn = df_pass_dvn[(df_pass_dvn["ОГРН"] == 1215000036305)]

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

    # Выделяем подразделение
    df_pass_dvn["Подразделение"] = df_pass_dvn["Структурное подразделение"].apply(
        lambda x: re.search(r"ОСП \d", x)[0] if re.match(r"^ОСП \d.*$", x) else "Ленинградская 9"
    )
    df_pass_dvn = df_pass_dvn.rename(columns={"Структурное подразделение": "Отделение"})

    df_pass_dvn = (
        df_pass_dvn.assign(count=lambda x: ~x["ФИО пациента"].isna())
        .drop(
            [
                "#",
                "ID подразделения",
                "Медицинская организация диспансеризации",
                "ОГРН",
                "Причина закрытия",
                "Процент прохождения",
                "Вид обследования",
                "Статус актуальный",
                "Дата обновления статуса",
                "Текст сообщения",
                "Дата создания карты диспансеризации",
                "Отделение",
                "Номер МКАБ",
                "ФИО пациента",
                "Врач подписывающий заключение диспансеризации",
                "Дата закрытия карты диспансеризации",
            ],
            axis=1,
        )
        .groupby(["Подразделение", "День недели"], observed=True)
        .count()
        .reset_index()
    )

    df_pass_dvn = df_pass_dvn.loc[df_pass_dvn["count"] != 0]

    # df_pass_dvn["Итог"] = df_pass_dvn[["Подразделение", "count"]].groupby("Подразделение").cum
    df_pass_dvn = df_pass_dvn.pivot(index="Подразделение", columns="День недели", values="count")
    df_pass_dvn["Итого"] = df_pass_dvn.sum(axis=1, skipna=True).astype(int)

    print(df_pass_dvn)

    # План на неделю задаётся вручную
    planned = {
        "Ленинградская 9": 630,
        "ОСП 1": 2070,
        "ОСП 2": 450,
        "ОСП 3": 1020,
        "ОСП 4": 1000,
        "ОСП 5": 560,
        "ОСП 6": 650,
        "ОСП 7": 530,
    }

    df_pass_dvn["План"] = pd.Series(data=planned).tolist()
    # df_pass_dvn = df_pass_dvn.rename(columns={"count": "Факт"})

    print(df_pass_dvn)

    try:
        os.mkdir(metric_path)
    except FileExistsError:
        pass

    utils.save_to_excel(
        df_pass_dvn,
        metric_path
        + "\\Свод по закрытой диспансеризации "
        + str(first_date)
        + "_"
        + str(yesterday_date)
        + ".xlsx",
        True,
    )

    # Агрегация для дашборда
    df_pass_dvn.loc["ПОКБ"] = df_pass_dvn.sum(numeric_only=True)

    df_pass_dvn["% по показателю 7"] = round(
        df_pass_dvn["Итого"] / df_pass_dvn["План"] * 100
    ).astype(int)
    df_pass_dvn = df_pass_dvn.drop(["План", "Итого"], axis=1)

    df_pass_dvn = df_pass_dvn.drop(
        [
            "Понедельник",
            "Вторник",
            "Среда",
            "Четверг",
            "Пятница",
            "Суббота",
            "Воскресенье",
            "План",
            "Итого",
        ],
        axis=1,
        errors="ignore",
    )
    print(df_pass_dvn)
    utils.save_to_excel(df_pass_dvn, metric_path + "\\agg_7.xlsx", True)


start_bi_report_saving()
analyze_data()
