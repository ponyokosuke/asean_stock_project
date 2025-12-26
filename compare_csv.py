import pandas as pd
import sys
import os

def load_codes(file_path):
    """CSVファイルを読み込んで、銘柄コードのセット(集合)を返す"""
    if not os.path.exists(file_path):
        print(f"エラー: ファイル '{file_path}' が見つかりません。")
        sys.exit(1)
        
    try:
        # ヘッダーなし(header=None)として読み込む
        df = pd.read_csv(file_path, header=None)
        # 文字列に変換し、前後の空白を削除してセットに格納
        codes = set(df[0].astype(str).str.strip())
        return codes
    except Exception as e:
        print(f"エラー: '{file_path}' の読み込みに失敗しました。({e})")
        sys.exit(1)

def main():
    # 引数チェック
    if len(sys.argv) < 3:
        print("使い方: python compare_csv.py <ファイル1> <ファイル2>")
        print("例: python compare_csv.py asean_list.csv IT_Targets_SG_20251226.csv")
        
        # 引数がない場合は、デフォルトで指定されたファイル名を使用する（テスト用）
        file1 = "asean_list.csv"
        file2 = "IT_Targets_SG_20251226.csv"
        print(f"\n引数が指定されていないため、デフォルトのファイルを使用します:\n1: {file1}\n2: {file2}\n")
    else:
        file1 = sys.argv[1]
        file2 = sys.argv[2]

    # ファイル読み込み
    print(f"--- 読み込み中 ---")
    set1 = load_codes(file1)
    set2 = load_codes(file2)

    print(f"ファイル1 ({file1}): {len(set1)} 件")
    print(f"ファイル2 ({file2}): {len(set2)} 件")
    
    # 共通のコード
    common = set1 & set2
    print(f"共通するコード: {len(common)} 件")

    # ---------------------------------------------------------
    # 差分の計算
    # ---------------------------------------------------------
    
    # set1 にだけあって、set2 にないもの
    only_in_1 = sorted(list(set1 - set2))
    
    # set2 にだけあって、set1 にないもの
    only_in_2 = sorted(list(set2 - set1))

    # ---------------------------------------------------------
    # 結果の出力
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print(f"■ [{file1}] にのみ存在するコード (差分: {len(only_in_1)} 件)")
    print("="*60)
    if only_in_1:
        print(", ".join(only_in_1))
        # 縦に並べて見たい場合は以下のコメントアウトを外してください
        # for c in only_in_1: print(c)
    else:
        print("(なし - すべてファイル2に含まれています)")

    print("\n" + "="*60)
    print(f"■ [{file2}] にのみ存在するコード (差分: {len(only_in_2)} 件)")
    print("="*60)
    if only_in_2:
        print(", ".join(only_in_2))
    else:
        print("(なし - すべてファイル1に含まれています)")
    print("\n" + "="*60)

if __name__ == "__main__":
    main()