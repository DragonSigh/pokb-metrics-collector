import os

from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service

current_path = os.path.abspath(os.getcwd())
system_path = ""
reports_path = os.path.join(current_path, "reports", "downloads", system_path)

# Опции для веб-драйвера
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
options.add_argument("--disable-extensions")
options.add_argument("--disable-popup-blocking")
#options.add_argument("--headless=new")
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": reports_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
    },
)

# Выбираем драйвер браузера и устанавливаем его опции
service = Service("C:\\chromedriver\\chromedriver.exe")
browser = webdriver.Chrome(options=options, service=service)
actions = ActionChains(browser)

# Период: с начала недели по сегодняшний день
first_date = date.today() - timedelta(days=date.today().weekday())
last_date = date.today()
yesterday_date = date.today() - timedelta(days=1)

# Если сегодня понедельник, то берем всю прошлую неделю
monday = date.today() - timedelta(days=date.today().weekday())
if date.today() == monday:
    first_date = monday - timedelta(days=7)  # начало прошлой недели
