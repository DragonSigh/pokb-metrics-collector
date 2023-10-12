import config
import pandas as pd
import gspread as gs

from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials


def shift_row_to_bottom(df, index_to_shift):
    idx = [i for i in df.index if i != index_to_shift]
    return df.loc[idx + [index_to_shift]]


# Настройки
path_to_credential = "pokb-399111-f04c71766977.json"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
spreadsheet_key = "1WR1EkI17EHSmXencOZzg9VC22NEUKtlpdq2ek-fNa5A"
wks = "Dashboard"

# Загрузка и объединение агрегаций показателей в датафреймы
df_7 = pd.read_excel(config.current_path + "\\reports\\Показатель 7" + "\\agg_7.xlsx", header=0)
df_22 = pd.read_excel(config.current_path + "\\reports\\Показатель 22" + "\\agg_22.xlsx", header=0)
# ИСКЛЮЧИЛИ ИЗ КР df_24
# pd.read_excel(config.current_path + "\\reports\\Показатель 24" + "\\agg_24.xlsx", header=0)
df_55 = pd.read_excel(config.current_path + "\\reports\\Показатель 55" + "\\agg_55.xlsx", header=0)

data_frames = [df_7, df_22, df_55]

df_final = reduce(
    lambda left, right: pd.merge(left, right, on=["Подразделение"], how="outer"), data_frames
)

df_final = (
    df_final.fillna(-1)
    .astype(
        {
            "Подразделение": str,
            "% по показателю 7": int,
            "% по показателю 22": int,
            "% по показателю 55": int,
        }
    )
    .replace(-1, "нет данных")
)

df_final = shift_row_to_bottom(df_final, 8)

print(df_final)

# Заливка в таблицу Google
gc = gs.authorize(credentials)
spreadsheet = gc.open_by_key(spreadsheet_key)
values = [df_final.columns.values.tolist()]
values.extend(df_final.values.tolist())
spreadsheet.values_update(wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values})
