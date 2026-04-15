import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def main():
    # 1. TDnetから適時開示情報を取得
    url = "https://www.release.tdnet.info/inbs/I_main_00.html"
    storage_options = {'User-Agent': 'Mozilla/5.0'}

    try:
        tables = pd.read_html(url, storage_options=storage_options)
        df = max(tables, key=len)

        if isinstance(df.columns[0], (int, float)):
            df.columns = df.iloc[0]
            df = df[1:]

        time_column = '時刻'
        if time_column in df.columns:
            filtered_df = df[df[time_column] >= '15:30']
        else:
            print(f"エラー: '{time_column}'列が見つかりません。")
            filtered_df = df 

        filtered_df = filtered_df.fillna('')

    except Exception as e:
        print(f"データ取得エラー: {e}")
        return

    print(f"取得したデータ件数: {len(filtered_df)}件")
    if len(filtered_df) == 0:
        print("15:30以降の開示情報はありませんでした。")
        return

    # 2. Googleスプレッドシートへの書き込み
    try:
        creds_json = os.environ.get('GCP_CREDENTIALS')
        creds_dict = json.loads(creds_json)

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)

        sheet_url = os.environ.get('SPREADSHEET_URL')
        workbook = gc.open_by_url(sheet_url)
        worksheet = workbook.sheet1

        worksheet.clear()
        data_to_write = [filtered_df.columns.values.tolist()] + filtered_df.values.tolist()
        worksheet.update(data_to_write)

        print("スプレッドシートへの書き込みが完了しました。")

    except Exception as e:
        print(f"スプレッドシート書き込みエラー: {e}")

if __name__ == "__main__":
    main()
