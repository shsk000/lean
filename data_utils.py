"""
データ関連のユーティリティ関数
"""
import yfinance as yf
import pandas as pd
import numpy as np
from data_loader import LocalDataLoader


def get_local_symbols():
    """ローカルデータから利用可能な銘柄リストを取得"""
    try:
        loader = LocalDataLoader()
        symbols = loader.get_available_symbols()
        print(f"ローカルデータから {len(symbols)} 銘柄を取得しました")
        return symbols
    except Exception as e:
        print(f"✗ ローカルデータの読み込みに失敗しました: {e}")
        print("data_collector.py を実行してデータを準備してください")
        exit(1)


def download_stock_data(symbol, start_date, end_date):
    """個別銘柄のデータをダウンロード"""
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)
        if df is None or df.empty or len(df) < 10:
            return symbol, None, "データ不足"
        
        if 'Volume' not in df.columns:
            return symbol, None, "Volumeカラムなし"
        
        volume_data = df['Volume'].dropna()
        if len(volume_data) == 0:
            return symbol, None, "出来高データなし"
        
        # より安全な平均値計算
        try:
            # 明示的にスカラー値として取得
            mean_series = volume_data.mean()
            if hasattr(mean_series, 'iloc'):
                # pandas Seriesの場合
                mean_value = float(mean_series.iloc[0])
            else:
                # スカラー値の場合
                mean_value = float(mean_series)
            
            if np.isnan(mean_value) or mean_value <= 0:
                return symbol, None, "出来高平均値が無効"
            return symbol, mean_value, "成功"
        except Exception as e:
            return symbol, None, f"出来高計算エラー: {e}"
        
    except Exception as e:
        return symbol, None, f"エラー: {e}"


def get_high_volume_symbols_from_local_data(max_symbols=None):
    """ローカルデータから高出来高銘柄を取得"""
    try:
        loader = LocalDataLoader()
        symbols = loader.get_available_symbols()
        
        if max_symbols and len(symbols) > max_symbols:
            symbols = symbols[:max_symbols]
            print(f"銘柄数を {max_symbols} に制限しました")
        
        print(f"高出来高銘柄 {len(symbols)} 銘柄をローカルデータから取得")
        return symbols
        
    except Exception as e:
        print(f"✗ ローカルデータからの銘柄取得に失敗: {e}")
        exit(1)


def download_backtest_data(symbol, start_date, end_date):
    """バックテスト用のデータをダウンロード（単一銘柄用にカラムを正規化）"""
    try:
        # group_by='ticker'は使わず、単一銘柄として取得
        df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False, progress=False)

        if df.empty or len(df) < 50:
            return symbol, None, f"データ不足 ({len(df)}日)"

        df.dropna(inplace=True)
        if len(df) < 50:
            return symbol, None, f"データ不足 ({len(df)}日)"

        # カラム名がMultiIndexやタプルの場合は1段目だけ抽出
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        else:
            # タプル文字列の場合も対応
            new_columns = []
            for col in df.columns:
                if isinstance(col, tuple):
                    new_columns.append(col[0])
                elif isinstance(col, str) and col.startswith("('") and col.endswith("')"):
                    # 文字列化されたタプル
                    col_clean = col.strip("()'\"")
                    if ',' in col_clean:
                        first_part = col_clean.split(',')[0].strip().strip("'\"")
                        new_columns.append(first_part)
                    else:
                        new_columns.append(col)
                else:
                    new_columns.append(col)
            df.columns = new_columns

        # カラム名のマッピング（大文字小文字の違いに対応）
        column_mapping = {
            'open': 'Open',
            'high': 'High', 
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume',
            'adj close': 'Adj Close',
            'adj_close': 'Adj Close'
        }
        normalized_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in column_mapping:
                normalized_columns.append(column_mapping[col_lower])
            else:
                normalized_columns.append(col)
        df.columns = normalized_columns

        # 必要なカラムが存在するかチェック
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return symbol, None, f"必要なカラムが不足: {missing_columns}"

        # データの品質チェック
        if df['Volume'].sum() == 0:
            return symbol, None, "出来高データが全て0"

        return symbol, df, "成功"
    except Exception as e:
        return symbol, None, f"エラー: {e}"