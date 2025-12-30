import yfinance as yf
import pandas as pd

def inspect_yfinance_data(ticker_symbol):
    """
    指定した銘柄のyfinance取得可能データを調査・表示する関数
    """
    print(f"========== {ticker_symbol} データ取得項目調査 ==========")
    
    # データ取得
    ticker = yf.Ticker(ticker_symbol)
    
    # ---------------------------------------------------------
    # [1] 企業情報・財務指標 (ticker.info)
    # ---------------------------------------------------------
    print(f"\n{'='*10} [1] 基本情報・指標 (ticker.info) {'='*10}")
    try:
        info = ticker.info
        # キーをソートして表示
        for key in sorted(info.keys()):
            val = info[key]
            # 長すぎる説明文などは省略して表示
            if isinstance(val, str) and len(val) > 50:
                val = val[:50] + "..."
            print(f"  - {key:<35}: {val}")
    except Exception as e:
        print(f"  エラー: infoが取得できませんでした ({e})")

    # ---------------------------------------------------------
    # [2] 財務諸表 (Financials / Balance Sheet / Cashflow)
    # ---------------------------------------------------------
    print(f"\n{'='*10} [2] 財務諸表 (項目一覧) {'='*10}")
    
    # 表示用のヘルパー関数
    def print_financial_items(title, df):
        print(f"\n  --- {title} ---")
        if df is not None and not df.empty:
            # 日付（列名）を表示
            latest_date = df.columns[0]
            print(f"  (最新データ日付: {latest_date.strftime('%Y-%m-%d')})")
            
            # 項目名（インデックス）とその値を表示
            items = sorted(df.index.tolist())
            for item in items:
                # 最新の値をサンプルとして表示
                val = df.loc[item].iloc[0]
                # 数値ならカンマ区切り、それ以外はそのまま
                val_str = f"{val:,.0f}" if isinstance(val, (int, float)) else str(val)
                print(f"    - {item:<40}: {val_str}")
        else:
            print("    (データなし)")

    # 損益計算書
    print_financial_items("損益計算書 (Financials)", ticker.financials)
    
    # 貸借対照表
    print_financial_items("貸借対照表 (Balance Sheet)", ticker.balance_sheet)
    
    # キャッシュフロー計算書（追加しました）
    print_financial_items("キャッシュフロー (Cashflow)", ticker.cashflow)

    # ---------------------------------------------------------
    # [3] 株主情報
    # ---------------------------------------------------------
    print(f"\n{'='*10} [3] 株主情報 {'='*10}")
    
    print("\n  --- 主要株主概況 (Major Holders) ---")
    if ticker.major_holders is not None and not ticker.major_holders.empty:
        # 見やすく整形して表示
        print(ticker.major_holders.to_string(index=False, header=False))
    else:
        print("    (データなし)")

    print("\n  --- 機関投資家 (Institutional Holders) [Top 5] ---")
    if ticker.institutional_holders is not None and not ticker.institutional_holders.empty:
        # 上位5件のみ表示
        print(ticker.institutional_holders.head(5).to_string(index=False))
    else:
        print("    (データなし)")
    
    print(f"\n{'='*40}")

# --- 実行 ---
# 確認したい銘柄コードを指定
target_code = "BIX.SI" 
inspect_yfinance_data(target_code)

# "D05.SI" DBS
# "40V.SI" Alset International Limited
# "F9D.SI" Boustead Singapore Limited
# "I07.SI" ISDN Holdings Limited


# 要チェックリスト

# "BQC.SI" A-Smart Holdings Ltd.
# "BIX.SI" Ellipsiz Ltd