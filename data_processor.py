# data_processor.py
import pandas as pd
from datetime import datetime

# format_shareholders 関数は変更なし（そのまま残す）
def format_shareholders(holders_data, data_type="institutional"):
    """
    株主データを '名前: 保有比率%' の形式に整形する関数
    """
    if holders_data is None or holders_data.empty:
        return None

    result_lines = []
    
    try:
        if data_type in ["institutional", "insider"]:
            name_col = None
            pct_col = None
            for col in holders_data.columns:
                col_lower = str(col).lower()
                if "holder" in col_lower or "insider" in col_lower:
                    name_col = col
                if "%" in col_lower or "pct" in col_lower or "out" in col_lower:
                    pct_col = col
            
            if name_col and pct_col:
                for index, row in holders_data.head(10).iterrows():
                    name = str(row[name_col])
                    val = row[pct_col]
                    try:
                        val_float = float(val)
                        if val_float < 1.0: 
                            val_float = val_float * 100
                        line = f"{name}: {val_float:.2f}%"
                    except:
                        line = f"{name}: {val}"
                    result_lines.append(line)

        elif data_type == "major":
            if len(holders_data.columns) == 1:
                for index, row in holders_data.head(5).iterrows():
                    desc = str(index)
                    val = row.iloc[0]
                    try:
                        if isinstance(val, str) and '%' in val:
                             line = f"{desc}: {val}"
                        else:
                             val_float = float(val)
                             if val_float <= 1.0:
                                 line = f"{desc}: {val_float:.2%}"
                             else:
                                 line = f"{desc}: {val}"
                    except:
                        line = f"{desc}: {val}"
                    result_lines.append(line)

            elif len(holders_data.columns) >= 2:
                for index, row in holders_data.head(5).iterrows():
                    val_raw = row.iloc[0]
                    desc_raw = row.iloc[1]
                    if len(str(desc_raw)) > len(str(val_raw)):
                        name = str(desc_raw)
                        val = str(val_raw)
                    else:
                        name = str(val_raw)
                        val = str(desc_raw)
                    if name:
                        line = f"{name}: {val}"
                        result_lines.append(line)

    except Exception as e:
        return f"Error parsing: {e}"

    if not result_lines:
        return None
        
    return "\n".join(result_lines)


def extract_data(code, raw_data):
    """
    Yahoo Financeの生データから必要な項目を抽出・計算する
    """
    info = raw_data.get("info", {})
    bs = raw_data.get("balance_sheet")
    inc = raw_data.get("financials")
    
    inst_holders = raw_data.get("institutional_holders")
    major_holders = raw_data.get("major_holders")
    
    shareholder_text = "Not Available"
    text_major = format_shareholders(major_holders, "major")
    if text_major:
        shareholder_text = text_major
    else:
        text_inst = format_shareholders(inst_holders, "institutional")
        if text_inst:
            shareholder_text = text_inst

    latest_date = None
    if bs is not None and not bs.empty:
        latest_date = bs.columns[0]
    elif inc is not None and not inc.empty:
        latest_date = inc.columns[0]

    def get_fin_value(df, key):
        if (df is not None and not df.empty and 
            key in df.index and 
            latest_date is not None and 
            latest_date in df.columns):
            return df.loc[key, latest_date]
        return 0

    # --- 損益計算書 ---
    revenue = get_fin_value(inc, "Total Revenue")
    
    pretax_income = get_fin_value(inc, "Pretax Income")
    operating_income = get_fin_value(inc, "Operating Income")
    gross_profit = get_fin_value(inc, "Gross Profit")
    
    profit = pretax_income if pretax_income != 0 else operating_income

    net_profit_owners = get_fin_value(inc, "Net Income")
    if net_profit_owners == 0:
        net_profit_owners = get_fin_value(inc, "Net Income Common Stock")

    net_profit_group = get_fin_value(inc, "Net Income Including Noncontrolling Interests")
    if net_profit_group == 0:
         net_profit_group = get_fin_value(inc, "Net Income Continuous Operations")
    
    minority_interest = get_fin_value(bs, "Minority Interest")
    if net_profit_group == 0 and net_profit_owners != 0:
        net_profit_group = net_profit_owners

    # --- 貸借対照表 ---
    stockholders_equity = get_fin_value(bs, "Stockholders Equity")
    total_assets = get_fin_value(bs, "Total Assets")
    
    total_equity = get_fin_value(bs, "Total Equity Gross Minority Interest")
    if total_equity == 0 and stockholders_equity != 0:
        total_equity = stockholders_equity + minority_interest

    # --- 負債 (Loan) の高度な取得ロジック ---
    
    # 1. Current Loan (短期借入金) - ※内部計算用
    current_loan = get_fin_value(bs, "Short Long Term Debt")
    if current_loan == 0:
        current_loan = get_fin_value(bs, "Current Debt")
        if current_loan == 0:
            current_loan = get_fin_value(bs, "Short Term Debt")

    # 2. Non-Current Loan (長期借入金) - ※内部計算用
    non_current_loan = get_fin_value(bs, "Long Term Debt")

    # 3. Loan (総有利子負債)
    loan = get_fin_value(bs, "Total Debt")
    
    # リース債務があれば引く (Net Loanにする)
    capital_lease = get_fin_value(bs, "Capital Lease Obligations")
    if loan != 0 and capital_lease != 0:
        if loan > capital_lease:
            loan = loan - capital_lease

    # もし Total Debt が取れなかったら足し算で補完
    if loan == 0:
        loan = current_loan + non_current_loan

    # --- 比率計算 ---
    debt_equity_ratio = None
    if total_equity and total_equity != 0 and total_assets and total_assets != 0:
         # Debt/Equity = (総資産 - 純資産) / 純資産
         total_liabilities = total_assets - total_equity
         debt_equity_ratio = (total_liabilities / total_equity)

    loan_equity_ratio = None
    if total_equity and total_equity != 0 and loan is not None:
         loan_equity_ratio = (loan / total_equity)


    # --- 基本情報 ---
    fy_date = None
    if info.get('lastFiscalYearEnd'):
        try:
            fy_date = datetime.fromtimestamp(info['lastFiscalYearEnd'])
        except:
            pass

    officers = info.get('companyOfficers', [])
    top_exec = "N/A"
    if officers:
        for officer in officers:
            title = officer.get('title', '').lower()
            if 'ceo' in title or 'chairman' in title:
                top_exec = f"{officer.get('name')} ({officer.get('title')})"
                break
        if top_exec == "N/A" and len(officers) > 0:
            top_exec = officers[0].get('name')

    address_parts = [
        info.get('address1'), info.get('address2'),
        info.get('city'), info.get('country'), info.get('zip')
    ]
    full_address = ", ".join([str(a) for a in address_parts if a])

    sector = info.get('sector')
    industry = info.get('industry')
    
    currency = info.get('financialCurrency')
    if not currency:
        currency = info.get('currency', 'N/A')
    if currency == 'CNY':
        currency = 'RMB (CNY)'
    
    website = info.get('website', '')

    # --- 結果の辞書作成 ---
    result = {
        "Name of Company": info.get('longName'),
        "Code": code,
        "Currency": currency,
        "Website": website,
        "Major Shareholders": shareholder_text,
        "FY": fy_date,
        "REVENUE": revenue,
        
        "PROFIT": profit,
        "GROSS PROFIT": gross_profit,
        "OPERATING PROFIT": operating_income,
        
        "NET PROFIT (Group)": net_profit_group,
        "NET PROFIT (Shareholders)": net_profit_owners,
        
        "Minority Interest": minority_interest,
        "Shareholders' Equity": stockholders_equity,
        "Total Equity": total_equity,
        "TOTAL ASSET": total_assets,
        
        "Debt/Equity(%)": debt_equity_ratio,
        
        "Loan": loan, # Current/Non-Current は出力しない
        
        "Loan/Equity (%)": loan_equity_ratio,
        
        "Summary of Business": info.get('longBusinessSummary'),
        "Chairman / CEO": top_exec,
        "Address": full_address,
        "Contact No.": info.get('phone'),
        "Number of Employee": info.get('fullTimeEmployees'),
        
        "Category Classification/ShareInvestor": sector if sector else "Not Available",
        "Sector & Industry ShareInvestor": industry if industry else "Not Available"
    }

    return result

def format_for_excel(df):
    """
    Excel出力用に整形（数値のまま保持する）
    """
    print("データを百万単位(数値)に変換しています...")
    
    # Current Loan, Non-Current Loan を削除
    money_cols = [
        'REVENUE', 'PROFIT', 'GROSS PROFIT', 'OPERATING PROFIT', 
        'NET PROFIT (Group)', 'NET PROFIT (Shareholders)',
        'Minority Interest',
        "Shareholders' Equity", 'Total Equity', 'TOTAL ASSET',
        'Loan' 
    ]
    divisor = 1000000.0

    for col in money_cols:
        if col in df.columns:
            # 数値(float)に変換
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 100万で割る
            df[col] = df[col] / divisor

    # 比率カラムも数値化を確実にする
    pct_cols = ["Debt/Equity(%)", "Loan/Equity (%)"]
    for col in pct_cols:
         if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 列名を変更して「(Mil)」を追記
    rename_map = {c: f"{c} (Mil)" for c in money_cols}
    df = df.rename(columns=rename_map)

    # 日付を文字列形式に変換 (YYYY/MM/DD)
    print("日付を YYYY/MM/DD 形式の文字列に変換しています...")
    date_cols = ['FY']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y/%m/%d').fillna('')

    return df