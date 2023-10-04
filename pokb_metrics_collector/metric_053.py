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

metric_path = config.current_path + "\\reports\\Показатель 53"


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
    df_smp_disp = pd.read_excel(
        config.reports_path
        + "\\Мониторинг выезда скорой помощи к пациентам, состоящих на д-учете.xlsx",
        skiprows=1,
        header=0,
    )
    # 1.2 http://bi.mz.mosreg.ru/#form/dispensary_patients
    # Детализация по картам диспансерного наблюдения
    # Можно выгружать раз в 2-3 недели
    df_dispensary_patients = pd.read_excel(
        config.reports_path + "\\Детализация по картам диспансерного наблюдения.xlsx",
        skiprows=1,
        header=0,
    )

    # 2. Выставляем фильтры в отчете 1.1

    # Предварительная  обработка

    # Только Подольская ОКБ
    df_smp_disp = df_smp_disp[(df_smp_disp["ОГРН"] == 1215000036305)]

    # Форматирование даты
    df_smp_disp["Дата и время приема вызова"] = pd.to_datetime(
        df_smp_disp["Дата и время приема вызова"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date

    # 2.1 Период выставляем отчетный (с понедельника и по сегодняшний день,
    # если отчет за неделю, тогда с понедельника по понедельник)
    df_smp_disp = df_smp_disp[
        (df_smp_disp["Дата и время приема вызова"] >= first_date)
        & (df_smp_disp["Дата и время приема вызова"] <= last_date)
    ]

    # 2.2 В столбце Возраст устанавливаем интервал с 18 до 59 (для женщин с 18 до 54)
    df_smp_disp = df_smp_disp[
        ((df_smp_disp["Пол"] == "Женский") & (df_smp_disp["Возраст"].between(18, 55)))
        | ((df_smp_disp["Пол"] == "Мужской") & (df_smp_disp["Возраст"].between(18, 60)))
    ]

    # 2.3 В столбце Сопоставление диагноза СМП и диагноза ДН выставляем фильтр ДА
    df_smp_disp = df_smp_disp[df_smp_disp["Сопоставление диагноза СМП и и диагноза ДН"] == "Да"]

    # 2.4 Убираем дубли
    df_smp_disp = df_smp_disp.drop_duplicates(subset=["Фамилия", "Возраст"], keep="first")

    df_smp_disp = df_smp_disp.drop(
        [
            "#",
            "Наименование мед.организации",
            "ОГРН",
            "Пол",
            "Диагнозы из карт ДН",
            "Дата и время приема вызова",
            "Дата и время прибытия на выезд",
            "Дата и время окончания вызова",
            "Затраченное время",
            "Диагноз, поставленный СМП",
            "Сопоставление диагноза СМП и и диагноза ДН",
            "Сопоставление диагноза СМП и и диагноза по 168н",
            "Тип вызова по поводу",
            "Категория повода к вызову",
            "Результат выезда",
            "Стационар/травмпункт",
            "Дата и время госпитализации",
        ], axis=1
    )

    # 3. Выставляем фильтры в отчете 1.2
    # 3.1 Медицинская организация "Подольская ОКБ"
    df_dispensary_patients = df_dispensary_patients[(df_dispensary_patients["ОГРН"] == 1215000036305)]
    # 3.2 Группа диагнозов E00-99, I00-99, J00-99, K00-93

    df_dispensary_patients["Фамилия"] = df_dispensary_patients[
        "Фамилия И.О. Больного (И.О. кратко)"
    ].apply(lambda x: x.split(" ")[0])

    df_dispensary_patients = df_dispensary_patients.drop(
        [
            "#",
            "ОГРН",
            "ЛПУ",
            "lpuid",
            "Пол",
            "Код врача из карты ДН",
            "Профиль врача из карты ДН",
            "Участок прикрепления",
            "ОГРН прикрепления",
            "Прикрепление (Юр.лцо)",
            "Телефон",
            "Шифр д-за по МКБ-Х",
            "Сопутствующий диагноз",
            "Инв.гр., отм.о льготах",
            "Даты осмотра в текущем году по основному диагнозу  ДН",
            "Количество наблюдений с начала года",
            "Даты осмотра в предыдущем году по основному диагнозу  ДН",
            "Дата предпоследнего посещения",
            "ТИП карты",
            "Коморбидность (наличие более одного диагноза из групп риска у данного пациента)",
            "Дата последнего осмотра по основному диагнозу  ДН",
            "Дата последнего посещения у терапевта или эндокринолога",
            "Рекомендуемая  дата следующего осмотра по основному диагнозу  ДН",
            "Даты использования шаблона ДН",
            "Дата последнего использования шаблона ДН",
            "Дата последнего протокола ТМК",
            "Дата снятия с учета",
            "Причина",
            "Дата смерти пациента",
            "Необходимо закрыть карту ДН",
            "Необходимо вызвать на ДН",
            "Дней с последнего посещения по дн",
            "Группа диагноза",
            "Не посещал более 2 лет",
            "patient_id",
            "period",
            "Приоритетность вызова на ДН (в баллах, чем выше, тем приоритетнее)",
            "ID подразделения прикрепления",
            "mkabid",
        ], axis=1
    )

    df_final = (
        df_smp_disp.merge(
            df_dispensary_patients,
            left_on=["Фамилия", "Возраст", "Дата постановки на д-учет"],
            right_on=["Фамилия", "Возраст", "Дата открытия карты ДН"],
            how="left",
            indicator=False,
        )
    )
    try:
        os.mkdir(metric_path)
    except FileExistsError:
        pass
    utils.save_to_excel(df_final, metric_path + "\\123.xlsx")


start_053_report_saving()
analyze_053_data()
