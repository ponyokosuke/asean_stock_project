# yfinance_client.py
import requests
import yfinance as yf
import time
import json

def get_stock_data(ticker_symbol):
    """
    指定された銘柄コードの全データを取得する
    """
    print(f"  データ取得中: {ticker_symbol} ...")
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # サーバー負荷を避けるため少し待つ
        time.sleep(1) 
        
        # 株主データ取得のデバッグ
        inst = ticker.institutional_holders
        major = ticker.major_holders
        
        return {
            "info": ticker.info,
            "balance_sheet": ticker.balance_sheet,
            "financials": ticker.financials,
            "major_holders": major,
            "institutional_holders": inst
        }
        
    except Exception as e:
        print(f"  エラー発生 ({ticker_symbol}): {e}")
        return None

# --- ★★★ 追加機能: Yahoo Financeから全銘柄リストを取得 ★★★ ---
def fetch_all_tickers_from_yahoo(region_code):
    """
    Yahoo FinanceのスクリーナーAPIを叩いて、
    指定された地域(sg, my, id, th, vn, ph)の「全株式銘柄」を取得する。
    """
    print(f"\nYahoo Financeから '{region_code}' 地域の全銘柄リストをダウンロード中...")
    
    # Yahoo FinanceのスクリーナーAPIエンドポイント
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    
    # 地域のマッピング (Yahoo Financeの定義に合わせる)
    # シンガポール: sg, マレーシア: my, インドネシア: id, タイ: th, ベトナム: vn, フィリピン: ph
    
    # ページネーション対応 (一度に最大250件しか取れないためループする)
    all_symbols = []
    offset = 0
    size = 250
    
    # ブラウザのふりをするヘッダー (これがないと弾かれる)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    while True:
        params = {
            "formatted": "false",
            "lang": "en-US",
            "region": region_code, # US, SG, MY ...
            "scrIds": "all_equities", # 「全ての株式」という条件
            "count": size,
            "start": offset
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f"  APIエラー: {response.status_code}")
                break
                
            data = response.json()
            quotes = data.get("finance", {}).get("result", [])[0].get("quotes", [])
            
            if not quotes:
                break # データがなくなったら終了
            
            for quote in quotes:
                symbol = quote.get("symbol")
                if symbol:
                    all_symbols.append(symbol)
            
            print(f"  ... {len(all_symbols)} 件取得済み")
            
            if len(quotes) < size:
                break # 最後のページ
                
            offset += size
            time.sleep(0.5) # 負荷軽減
            
        except Exception as e:
            print(f"  取得中にエラー発生: {e}")
            break
            
    print(f"完了: 合計 {len(all_symbols)} 件の銘柄が見つかりました。")
    return all_symbols