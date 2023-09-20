import config
import utils
import os

from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

browser = config.browser
actions = config.actions
reports_path = config.reports_path


def authorize(login_data: str, password_data: str):
    logger.debug("Начата авторизация")
    browser.get("http://bi.mz.mosreg.ru/login/")
    login_field = browser.find_element(By.XPATH, '//*[@id="login"]')
    login_field.send_keys(login_data)
    password_field = browser.find_element(By.XPATH, '//*[@id="password"]')
    password_field.send_keys(password_data)
    browser.find_element(
        By.XPATH, '//*[@id="isLoginBinding"]/form/div[4]/button'
    ).click()
    logger.debug("Авторизация пройдена")


def load_any_report(report_name, begin_date, end_date):
    logger.debug(
        f"Открываю {report_name} - выбран период:"
        f" с {begin_date.strftime('%d.%m.%Y')}"
        f" по {end_date.strftime('%d.%m.%Y')}"
    )
    browser.get("http://bi.mz.mosreg.ru/#form/" + report_name)
    WebDriverWait(browser, 60).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//input[@data-componentid='ext-datefield-3']")
        )
    )
    browser.execute_script(
        "var first_date = globalThis.Ext.getCmp('ext-datefield-3'); +\
                           first_date.setValue('"
        + begin_date.strftime("%d.%m.%Y")
        + "'); + \
                           first_date.fireEvent('select');"
    )
    browser.execute_script(
        "var last_date = globalThis.Ext.getCmp('ext-datefield-4'); +\
                           last_date.setValue('"
        + end_date.strftime("%d.%m.%Y")
        + "'); + \
                           last_date.fireEvent('select');"
    )
    WebDriverWait(browser, 300).until(
        EC.invisibility_of_element(
            (By.XPATH, '//div[@data-componentid="ext-toolbar-8"]')
        )
    )
    # Фильтр ОГРН
    # if report_name == 'pass_dvn':
    #    browser.execute_script("var ogrn_filter = globalThis.Ext.getCmp('ext-RTA-grid-textfilter-14'); +\
    #                        ogrn_filter.setValue('1215000036305'); + \
    #                        ogrn_filter.fireEvent('select');")
    browser.find_element(
        By.XPATH, "//button[@data-componentid='ext-button-12']"
    ).click()
    WebDriverWait(browser, 300).until(
        EC.invisibility_of_element(
            (By.XPATH, '//div[@data-componentid="ext-toolbar-8"]')
        )
    )


def export_report():
    logger.debug(f"Начинается сохранение файла с отчетом в папку: {reports_path}")
    try:
        os.mkdir(reports_path)
    except FileExistsError:
        pass
    # Нажимаем на кнопку "Выгрузить в Excel" и ожидаем загрузку файла
    browser.find_element(
        By.XPATH, "//button[@data-componentid='ext-button-13']"
    ).click()
    utils.download_wait(reports_path, 600, len(os.listdir(reports_path)) + 1)
    browser.find_element(
        By.XPATH,
        "/html/body/div[1]/div[2]/div/div/div/div[2]/div/div/div[1]/div[1]/div[2]/div/div[3]/div[4]",
    ).click()
    logger.debug("Сохранение файла с отчетом успешно")
