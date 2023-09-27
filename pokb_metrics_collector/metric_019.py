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

metric_path = config.current_path + "\\reports\\Показатель 19"


# @utils.retry_with_backoff(retries=5)
def start_019_report_saving():
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
        + "\\Мониторинг проведения телемедицинских консультаций [Пререкорд].xlsx"
    ):
        bi_emias.load_any_report("tk_cells", first_date, last_date)
        bi_emias.export_report()
    if not utils.is_actual_report_exist(
        config.reports_path + "\\Детализированный отчёт по врачам, проводящим ТМК.xlsx"
    ):
        bi_emias.load_any_report("tk_cells_doc_v2", first_date, last_date)
        bi_emias.export_report()
    if not utils.is_actual_report_exist(config.reports_path + "\\Отчет по расписанию врачей .xlsx"):
        bi_emias.load_any_report("shed_lpu", first_date, last_date)
        bi_emias.export_report()

    logger.debug("Выгрузка из BI ЕМИАС завершена")


def analyze_019_data():
    # Загружаем отчёты в датафреймы
    # 1.1 http://bi.mz.mosreg.ru/#form/tk_cells
    # (нужен для проверки использования шаблонов в кабинетах (ЛЛО, Справки онлайн и т.д.)
    df_tk_cells = pd.read_excel(
        config.reports_path
        + "\\Мониторинг проведения телемедицинских консультаций [Пререкорд].xlsx",
        skiprows=1,
        header=0,
    )
    # 1.2 http://bi.mz.mosreg.ru/#form/tk_cells_doc_v2 (детализированный по врачам)
    df_tk_cells_doc_v2 = pd.read_excel(
        config.reports_path + "\\Детализированный отчёт по врачам, проводящим ТМК.xlsx",
        skiprows=1,
        header=0,
    )
    # 1.3 http://bi.mz.mosreg.ru/#form/shed_lpu (нужен для сопоставления врача и подразделения)
    df_shed_lpu = pd.read_excel(
        config.reports_path + "\\Отчет по расписанию врачей .xlsx", skiprows=1, header=0
    )
    # Только Подольская ОКБ
    df_tk_cells = df_tk_cells[(df_tk_cells["ОГРН"] == 1215000036305)]
    df_tk_cells_doc_v2 = df_tk_cells_doc_v2[(df_tk_cells_doc_v2["ОГРН"] == 1215000036305)]
    df_shed_lpu = df_shed_lpu[(df_shed_lpu["ОГРН"] == 1215000036305)]
    # Убрать пробел из названия колонки
    df_shed_lpu = df_shed_lpu.rename(columns={'ФИО врача ': 'ФИО врача'})
    # Оставляем только врачей и подразделения для сопоставления
    df_shed_lpu = df_shed_lpu[["ФИО врача", "Подразделение"]]
    # Приводим подразделения к нормальному виду
    df_shed_lpu["Подразделение"] = df_shed_lpu["Подразделение"].apply(lambda x: x.split(" [")[0])
    # 2.1 В столбце Занято не должно быть нулей
    df_tk_cells = df_tk_cells.loc[df_tk_cells["Занято"] != 0]
    df_tk_cells_doc_v2 = df_tk_cells_doc_v2.loc[df_tk_cells_doc_v2["Занято"] != 0]

    df_final = df_tk_cells_doc_v2.merge(
        df_shed_lpu, left_on=["Врач"], right_on=["ФИО врача"], how="left", indicator=True
    )
    try:
        os.mkdir(metric_path)
    except FileExistsError:
        pass
    utils.save_to_excel(df_final, metric_path + "\\123.xlsx")


# start_019_report_saving()
analyze_019_data()
