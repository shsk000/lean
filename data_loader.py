"""
データローダー (Data Loader)
==========================

ローカルに保存された株式データを読み込み、
バックテスト実行時に高信頼性でデータを提供するモジュールです。

機能:
1. ローカルCSVファイルからOHLCVデータを読み込み
2. データ整合性チェック
3. 厳格なエラーハンドリング（データ不備時は即座に終了）
4. backtraderフォーマットへの変換

使用方法:
--------
from data_loader import LocalDataLoader

loader = LocalDataLoader()
symbols = loader.get_available_symbols()
data = loader.load_stock_data('AAPL')
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime
import backtrader as bt

class LocalDataLoader:
    """ローカル株式データ管理クラス"""
    
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        self.stocks_dir = os.path.join(data_dir, 'stocks')
        self.config_file = os.path.join(data_dir, 'config.json')
        self.symbols_file = os.path.join(data_dir, 'high_volume_symbols.json')
        
        self._validate_data_structure()
        self._load_configuration()
    
    def _validate_data_structure(self):
        """データ構造の妥当性チェック"""
        if not os.path.exists(self.data_dir):
            self._exit_with_error(f"データディレクトリが存在しません: {self.data_dir}")
        
        if not os.path.exists(self.stocks_dir):
            self._exit_with_error(f"株式データディレクトリが存在しません: {self.stocks_dir}")
        
        if not os.path.exists(self.config_file):
            self._exit_with_error(f"設定ファイルが存在しません: {self.config_file}")
        
        if not os.path.exists(self.symbols_file):
            self._exit_with_error(f"シンボルファイルが存在しません: {self.symbols_file}")
    
    def _load_configuration(self):
        """設定ファイルの読み込み"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            with open(self.symbols_file, 'r', encoding='utf-8') as f:
                self.symbols_info = json.load(f)
            
            print(f"✓ ローカルデータ設定読み込み完了")
            print(f"  - データ期間: {self.config['data_period']['start_date']} - {self.config['data_period']['end_date']}")
            print(f"  - 利用可能銘柄数: {self.symbols_info['total_count']}")
            
        except Exception as e:
            self._exit_with_error(f"設定ファイル読み込みエラー: {e}")
    
    def _exit_with_error(self, message):
        """エラー時の強制終了"""
        print(f"[FATAL ERROR] {message}")
        print("データの整合性に問題があります。data_collector.py を実行してデータを再構築してください。")
        sys.exit(1)
    
    def get_available_symbols(self):
        """利用可能な銘柄リストを取得"""
        return self.symbols_info['symbols']
    
    def get_data_period(self):
        """データ期間を取得"""
        return (
            self.config['data_period']['start_date'],
            self.config['data_period']['end_date']
        )
    
    def load_stock_data(self, symbol, start_date=None, end_date=None):
        """個別銘柄データの読み込み"""
        csv_path = os.path.join(self.stocks_dir, f"{symbol}.csv")
        
        if not os.path.exists(csv_path):
            self._exit_with_error(f"銘柄データが存在しません: {symbol} ({csv_path})")
        
        try:
            # CSVファイル読み込み（タイムゾーン情報を無視）
            df = pd.read_csv(csv_path, index_col=0)
            
            # 日付インデックスを明示的に変換（タイムゾーン情報を除去）
            try:
                # UTCで変換してからローカルタイムに変換
                df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
            except Exception:
                # フォールバック: 日付文字列から直接変換
                df.index = pd.to_datetime([str(d).split()[0] for d in df.index])
            
            print(f"✓ [{symbol}] インデックス変換完了")
            
            # データ整合性チェック
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self._exit_with_error(f"[{symbol}] 必要なカラムが不足: {missing_columns}")
            
            # 空データチェック
            if df.empty:
                self._exit_with_error(f"[{symbol}] データが空です")
            
            # 最小データ量チェック
            if len(df) < 100:
                self._exit_with_error(f"[{symbol}] データが不足しています ({len(df)}日 < 100日)")
            
            # 日付範囲フィルタリング
            if start_date:
                start_dt = pd.to_datetime(start_date)
                df = df[df.index >= start_dt]
            if end_date:
                end_dt = pd.to_datetime(end_date)
                df = df[df.index <= end_dt]
            
            # フィルタ後のデータチェック
            if df.empty:
                self._exit_with_error(f"[{symbol}] 指定期間のデータが存在しません (期間: {start_date} - {end_date})")
            
            # 必要なカラムのみ抽出
            df = df[required_columns]
            
            # 数値データの妥当性チェック
            if df.isnull().any().any():
                self._exit_with_error(f"[{symbol}] データに欠損値が含まれています")
            
            # 価格データの妥当性チェック
            if (df[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
                self._exit_with_error(f"[{symbol}] 価格データに無効な値が含まれています")
            
            # 出来高データの妥当性チェック
            if (df['Volume'] < 0).any():
                self._exit_with_error(f"[{symbol}] 出来高データに無効な値が含まれています")
            
            print(f"✓ [{symbol}] データ読み込み完了 ({len(df)}日)")
            return df
            
        except Exception as e:
            if "FATAL ERROR" in str(e):
                raise  # 既にエラーメッセージが出力されている場合は再発生
            self._exit_with_error(f"[{symbol}] データ読み込みエラー: {e}")
    
    def load_multiple_stocks(self, symbols, start_date=None, end_date=None):
        """複数銘柄データの一括読み込み"""
        print(f"複数銘柄データを読み込み中... ({len(symbols)}銘柄)")
        
        stocks_data = {}
        failed_symbols = []
        
        for symbol in symbols:
            try:
                data = self.load_stock_data(symbol, start_date, end_date)
                stocks_data[symbol] = data
            except SystemExit:
                # _exit_with_errorによる終了の場合は即座に終了
                raise
            except Exception as e:
                failed_symbols.append(symbol)
                print(f"✗ [{symbol}] 読み込み失敗: {e}")
        
        # 失敗があった場合は厳格に終了
        if failed_symbols:
            self._exit_with_error(f"以下の銘柄でデータ読み込みに失敗しました: {failed_symbols}")
        
        print(f"✓ 全銘柄データ読み込み完了 ({len(stocks_data)}銘柄)")
        return stocks_data
    
    def create_backtrader_data(self, symbol, start_date=None, end_date=None):
        """backtrader用データフィードの作成"""
        df = self.load_stock_data(symbol, start_date, end_date)
        return bt.feeds.PandasData(dataname=df, name=symbol)
    
    def get_symbol_info(self, symbol):
        """銘柄の詳細情報を取得"""
        for data in self.symbols_info['data_details']:
            if data['symbol'] == symbol:
                return data
        return None
    
    def validate_date_range(self, start_date, end_date):
        """指定された日付範囲の妥当性チェック"""
        config_start = self.config['data_period']['start_date']
        config_end = self.config['data_period']['end_date']
        
        if start_date < config_start:
            self._exit_with_error(f"開始日が利用可能範囲外です: {start_date} < {config_start}")
        
        if end_date > config_end:
            self._exit_with_error(f"終了日が利用可能範囲外です: {end_date} > {config_end}")
        
        if start_date >= end_date:
            self._exit_with_error(f"開始日が終了日以降です: {start_date} >= {end_date}")
        
        print(f"✓ 日付範囲チェック完了: {start_date} - {end_date}")

# テスト用関数
def test_data_loader():
    """データローダーのテスト"""
    try:
        loader = LocalDataLoader()
        symbols = loader.get_available_symbols()
        print(f"利用可能銘柄数: {len(symbols)}")
        
        if symbols:
            test_symbol = symbols[0]
            data = loader.load_stock_data(test_symbol)
            print(f"テスト銘柄 {test_symbol}: {len(data)}日のデータ")
            
        print("✓ データローダーテスト完了")
        
    except Exception as e:
        print(f"✗ データローダーテスト失敗: {e}")

if __name__ == "__main__":
    test_data_loader()