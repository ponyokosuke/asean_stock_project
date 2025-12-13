import yfinance as yf
import pandas as pd

def check_sectors(tickers):
    """
    指定された銘柄リストについて、yfinanceから取得できる
    Sector (セクター) と Industry (産業) を一覧表示する
    """
    print(f"=== yfinance セクター & インダストリー 取得テスト ===\n")
    
    results = []

    for ticker_symbol in tickers:
        print(f"Checking: {ticker_symbol} ...")
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            # 取得したい情報
            name = info.get('longName', 'N/A')
            sector = info.get('sector', 'Not Available')
            industry = info.get('industry', 'Not Available')
            
            results.append({
                "Code": ticker_symbol,
                "Name": name,
                "Sector": sector,
                "Industry": industry
            })
            
        except Exception as e:
            print(f"  Error: {e}")
            results.append({
                "Code": ticker_symbol,
                "Name": "Error",
                "Sector": "Error",
                "Industry": "Error"
            })

    # 結果をDataFrameにして表示
    df = pd.DataFrame(results)
    
    print("\n" + "="*60)
    print(" 取得結果一覧")
    print("="*60)
    # 表形式できれいに表示
    print(df.to_string(index=False))
    
    # ユニークなセクター一覧を表示
    print("\n" + "="*60)
    print(" 検出されたセクター一覧 (ユニーク)")
    print("="*60)
    unique_sectors = df['Sector'].unique()
    for s in sorted(unique_sectors):
        print(f"- {s}")

# --- テスト対象の銘柄リスト ---
# いろいろな業種の銘柄を混ぜています
test_tickers = [
    # 米国株 (基準となる分類)
    "AAPL",   # Apple (Technology)
    "MSFT",   # Microsoft (Technology)
    "AMZN",   # Amazon (Consumer Cyclical)
    "JPM",    # JPMorgan (Financial Services)
    "JNJ",    # Johnson & Johnson (Healthcare)
    "XOM",    # Exxon Mobil (Energy)
    "PG",     # Procter & Gamble (Consumer Defensive)
    
    # 日本株
    "7203.T", # トヨタ自動車
    "9984.T", # ソフトバンクG
    "8306.T", # 三菱UFJ
    
    # シンガポール株 (ASEAN)
    "D05.SI", # DBS Group (銀行)
    "Z74.SI", # Singtel (通信)
    "C09.SI", # City Developments (不動産)
    "U11.SI", # United Overseas Bank
]

# 実行
if __name__ == "__main__":
    check_sectors(test_tickers)