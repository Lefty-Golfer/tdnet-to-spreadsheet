import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def main():
    # --------------------------------------------------
    # 1. TDnetから適時開示情報を取得
    # --------------------------------------------------
    url = "https://www.release.tdnet.info/inbs/I_main_00.html"
    
    # スクレイピング時のアクセス拒否を防ぐためのヘッダー
    storage_options = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # ページ内のすべての表(テーブル)を読み込む
        tables = pd.read_html(url, storage_options=storage_options)
        
        # TDnetの構造上、通常は一番大きな表が開示情報のリストになります
        df = max(tables, key=len)
        
        # 列名が正しく認識されていない場合の簡易補正（TDnetの仕様変更に備えるため）
        # 仮の列名が数字になっている場合は、1行目を列名に昇格
        if isinstance(df.columns[0], (int, float)):
            df.columns = df.iloc[0]
            df = df[1:]
            
        # '時刻' 列が存在するか確認し、15:30以降でフィルタリング
        time_column = '時刻'
        if time_column in df.columns:
            filtered_df = df[df[time_column] >= '15:30']
        else:
            print(f"エラー: '{time_column}'列が見つかりません。サイトの構造が変わった可能性があります。")
            filtered_df = df # フォールバックとして全件出力
            
        # 欠損値（NaN）を空文字に変換（スプレッドシートへの書き込みエラーを防ぐため）
        filtered_df = filtered_df.fillna('')
        
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
        # GitHub Secretsから環境変数経由でJSON情報を読み込む
        creds_json = os.environ.get('GCP_CREDENTIALS')
        creds_dict = json.loads(creds_json)
        
        # APIの権限スコープを設定
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        
        # GitHub SecretsからURLを読み込み、シートを開く
        sheet_url = os.environ.get('SPREADSHEET_URL')
        workbook = gc.open_by_url(sheet_url)
        worksheet = workbook.sheet1 # 1つ目のシートを指定
        
        # 一度シートをクリア
        worksheet.clear()
        
        # ヘッダー（列名）とデータ本体を結合して一括書き込み
        data_to_write = [filtered_df.columns.values.tolist()] + filtered_df.values.tolist()
        worksheet.update(data_to_write)
        
        print("スプレッドシートへの書き込みが完了しました。")
        
    except Exception as e:
        print(f"スプレッドシート書き込みエラー: {e}")

if __name__ == "__main__":
    main()
