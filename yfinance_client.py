# yfinance_client.py
import yfinance as yf
import time

def get_stock_data(ticker_symbol):
    """
    指定された銘柄コードの全データを取得する
    """
    print(f"  データ取得中: {ticker_symbol} ...")
    
    try:
        # Tickerオブジェクトを作成
        ticker = yf.Ticker(ticker_symbol)
        
        # サーバー負荷を避けるため少し待つ
        time.sleep(1) 
        
        # --- ★★★ デバッグ用ログ (ここから) ★★★ ---
        # 株主データが取れているか確認して表示する
        inst = ticker.institutional_holders
        major = ticker.major_holders
        
        print(f"    [Debug] 機関投資家データ有無: {'あり' if inst is not None and not inst.empty else 'なし'}")
        print(f"    [Debug] 主要株主データ有無:   {'あり' if major is not None and not major.empty else 'なし'}")
        
        if inst is not None and not inst.empty:
            print(f"    [Debug] 機関投資家カラム名: {inst.columns.tolist()}")
        if major is not None and not major.empty:
            print(f"    [Debug] 主要株主カラム名:   {major.columns.tolist()}")
        # --- ★★★ デバッグ用ログ (ここまで) ★★★ ---

        # 必要なデータを辞書にまとめて返す
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