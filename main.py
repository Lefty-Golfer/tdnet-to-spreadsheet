import os
import json
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials

def main():
    # --------------------------------------------------
    # 1. TDnetから適時開示情報を取得（文字化け対策版）
    # --------------------------------------------------
    url = "https://www.release.tdnet.info/inbs/I_main_00.html"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # requestsを使って明示的にUTF-8で取得することで文字化けを防ぎます
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        
        # 取得したHTMLテキストをpandasに渡します
        tables = pd.read_html(response.text)
        df = max(tables, key=len)
        
        # 列名（ヘッダー）のセット
        if '時刻' not in df.columns:
            df.columns = df.iloc[0]
            df = df[1:]
            
        # 15:30以降のデータを抽出
        # 時刻列を文字列にして比較しやすくします
        df['時刻'] = df['時刻'].astype(str)
        filtered_df = df[df['時刻'] >= '15:30'].copy()
        
        # 全てのセルを文字列に変換（スプレッドシートへの転記エラーを防ぐ）
        filtered_df = filtered_df.astype(str)
        filtered_df = filtered_df.replace('nan', '')
        
    except Exception as e:
        print(f"データ取得エラー: {e}")
        return

    print(f"取得したデータ件数: {len(filtered_df)}件")
    if len(filtered_df) == 0:
        print("15:30以降の開示情報はありませんでした。")
        return

    # --------------------------------------------------
    # 2. Googleスプレッドシートへの書き込み
    # --------------------------------------------------
    try:
        creds_json = os.environ.get('GCP_CREDENTIALS')
        creds_dict = json.loads(creds_json)
        
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        
        sheet_url = os.environ.get('SPREADSHEET_URL')
        workbook = gc.open_by_url(sheet_url)
        worksheet = workbook.sheet1
        
        # シートを完全にリセットしてから書き込む
        worksheet.clear()
        
        # リスト形式に変換して一括更新
        data_to_write = [filtered_df.columns.values.tolist()] + filtered_df.values.tolist()
        worksheet.update(data_to_write)
        
        print("スプレッドシートへの書き込みが正常に完了しました。")
        
    except Exception as e:
        print(f"スプレッドシート書き込みエラー: {e}")

if __name__ == "__main__":
    main()
