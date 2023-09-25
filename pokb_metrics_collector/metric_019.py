import config
import utils
import bi_emias
import os
import json
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
        "Мониторинг проведения телемедицинских консультаций [Пререкорд].xlsx"
    ):
        bi_emias.load_any_report("tk_cells", first_date, last_date)
        bi_emias.export_report()

    bi_emias.load_any_report("tk_cells_doc_v2", first_date, last_date)
    bi_emias.export_report()

    bi_emias.load_any_report("shed_lpu", first_date, last_date)
    bi_emias.export_report()

    logger.debug("Выгрузка из BI ЕМИАС завершена")


start_019_report_saving()
