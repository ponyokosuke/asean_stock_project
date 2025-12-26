import pandas as pd
import yfinance as yf
import sys
import os
import time

def load_codes(file_path):
    """CSVファイルを読み込んで、銘柄コードのセット(集合)を返す"""
    if not os.path.exists(file_path):
        print(f"エラー: ファイル '{file_path}' が見つかりません。")
        sys.exit(1)
    try:
        df = pd.read_csv(file_path, header=None)
        # 文字列変換、空白削除
        codes = set(df[0].astype(str).str.strip())
        # 空行などを除外
        codes = {c for c in codes if c and str(c) != 'nan'}
        return codes
    except Exception as e:
        print(f"エラー: '{file_path}' の読み込みに失敗しました。({e})")
        sys.exit(1)

def fetch_company_info(code):
    """Yahoo Financeから会社名と概要を取得する"""
    try:
        ticker = yf.Ticker(code)
        info = ticker.info
        return {
            "Code": code,
            "Name": info.get('longName', 'N/A'),
            "Business Summary": info.get('longBusinessSummary', 'N/A'),
            "Industry": info.get('industry', 'N/A'),
            "Sector": info.get('sector', 'N/A')
        }
    except Exception as e:
        print(f"  取得エラー: {code} ({e})")
        return {
            "Code": code,
            "Name": "Error",
            "Business Summary": "取得失敗",
            "Industry": "",
            "Sector": ""
        }

def process_list(code_list, source_name):
    """コードリストを受け取り、詳細情報を取得してDataFrameにする"""
    data = []
    total = len(code_list)
    print(f"\n--- '{source_name}' にのみ存在する銘柄の情報を取得中 ({total}件) ---")
    
    for i, code in enumerate(sorted(code_list)):
        print(f"[{i+1}/{total}] Fetching: {code} ...")
        info = fetch_company_info(code)
        data.append(info)
        time.sleep(0.5) # 負荷軽減
        
    return pd.DataFrame(data)

def main():
    # デフォルトのファイル名
    default_file1 = "asean_list.csv"
    default_file2 = "IT_Targets_SG_20251226.csv"

    # 引数があればそれを使用
    file1 = sys.argv[1] if len(sys.argv) > 1 else default_file1
    file2 = sys.argv[2] if len(sys.argv) > 2 else default_file2

    print(f"比較対象1: {file1}")
    print(f"比較対象2: {file2}")

    # 1. リストの読み込み
    set1 = load_codes(file1)
    set2 = load_codes(file2)

    # 2. 差分の抽出
    only_in_1 = set1 - set2  # file1にあってfile2にない
    only_in_2 = set2 - set1  # file2にあってfile1にない

    print(f"\n共通: {len(set1 & set2)} 件")
    print(f"{file1} のみ: {len(only_in_1)} 件")
    print(f"{file2} のみ: {len(only_in_2)} 件")

    # 3. 情報取得 & DataFrame化
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()

    if only_in_1:
        df1 = process_list(only_in_1, file1)
    
    if only_in_2:
        df2 = process_list(only_in_2, file2)

    # 4. Excelへの保存
    output_filename = "Comparison_Result_with_Summary.xlsx"
    print(f"\nExcelファイルを作成しています: {output_filename} ...")
    
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            # シート1: File1のみ
            if not df1.empty:
                # 列の並び替え
                cols = ["Code", "Name", "Industry", "Sector", "Business Summary"]
                df1 = df1.reindex(columns=cols)
                df1.to_excel(writer, sheet_name='Only_in_File1', index=False)
            
            # シート2: File2のみ
            if not df2.empty:
                cols = ["Code", "Name", "Industry", "Sector", "Business Summary"]
                df2 = df2.reindex(columns=cols)
                df2.to_excel(writer, sheet_name='Only_in_File2', index=False)
                
        print("★★★ 完了しました ★★★")
        
    except Exception as e:
        print(f"保存エラー: {e}")

if __name__ == "__main__":
    main()