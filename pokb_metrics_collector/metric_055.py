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
def start_055_report_saving():
    # Получить путь к файлу с данными для авторизации
    credentials_path = os.path.join(config.current_path, "auth-bi-emias.json")
    f = open(credentials_path, "r", encoding="utf-8")
    data = json.load(f)
    f.close()
    if not utils.is_actual_report_exist(
        config.reports_path + "\\Детализированный отчет по справкам 095-у Освобождение(ЛПУ).xlsx"
    ):
        for _departments in data["departments"]:
            for _units in _departments["units"]:
                bi_emias.authorize(_units["login"], _units["password"])
        bi_emias.load_any_report("medical_certificate_tap_dp2", first_date, last_date)
        bi_emias.export_report()

    logger.debug("Выгрузка из BI ЕМИАС завершена")


def analyze_055_data():
    # Загружаем отчёт в датафрейм
    # http://bi.mz.mosreg.ru/#form/medical_certificate_tap_dp2
    # (Детализированный отчет по справкам 095/у Освобождение (ЛПУ))
    df_med_cert = pd.read_excel(
        config.reports_path + "\\Детализированный отчет по справкам 095-у Освобождение(ЛПУ).xlsx",
        skiprows=1,
        header=0,
    )
    # Только Подольская ОКБ
    df_med_cert = df_med_cert[(df_med_cert["ОГРН"] == 1215000036305)]
    # Отдельный датафрейм для агрегации в дашборд
    df_agg = df_med_cert
    # Признак "необходимо сформировать справку по форме 095/у" для ТАП = "1"
    df_med_cert = df_med_cert[
        df_med_cert['Признак "необходимо сформировать справку по форме 095/у" для ТАП'] == 1
    ]
    df_med_cert["Дата ТАП"] = pd.to_datetime(
        df_med_cert["Дата постановки диагноза"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date

    # Оставляем только опеределенные столбцы
    df_med_cert = df_med_cert[
        [
            "Подразделение",
            "Фамилия",
            "Дата рождения",
            "Дата ТАП",
            "Врач",
            "Номер справки",
        ]
    ]

    # Смотрим столбец Номер справки — столбец не должен быть пустым
    # Строки с пустым значением в данном столбце отдаем на отработку
    df_med_cert = df_med_cert[df_med_cert["Номер справки"].isnull()].drop("Номер справки", axis=1)

    df_med_cert["Отделение"] = df_med_cert["Подразделение"]
    df_med_cert["Подразделение"] = df_med_cert["Подразделение"].apply(utils.get_department)

    try:
        os.mkdir(metric_path)
    except FileExistsError:
        pass

    for department in df_med_cert["Подразделение"].unique():
        df_temp = (
            df_med_cert[df_med_cert["Подразделение"] == department]
            .drop(["Подразделение"], axis=1)
            .sort_values(["Отделение", "Врач"])
        )
        # Фильтрация датафрейма по уникальному значению в колонке
        utils.save_to_excel(df_temp, metric_path + "\\" + department + ".xlsx")

    # Агрегация
    df_agg = df_agg.reset_index()
    df_agg["Подразделение"] = df_agg["Подразделение"].apply(utils.get_department)
    df_agg = df_agg.groupby(["Подразделение", 'Признак "справка сформирована корректно"']).count()
    df_agg.loc["ПОКБ"] = df_agg.sum(numeric_only=True)
WIP
    print(df_agg)
    #df_agg["% по показателю 55"] = round(
    #    df_agg["Из них по регламенту"] / df_agg["Всего рецептов"] * 100
    #)

start_055_report_saving()
analyze_055_data()
