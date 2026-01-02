#!/usr/bin/env python3
"""
Crypto Market Analyzer - Binance Version
v3.0 - Google OAuth + User Watchlists
"""

import asyncio
import json
import logging
import os
import secrets
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import ccxt
import httpx
import numpy as np
import pandas as pd
import ta as ta_lib
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('analyzer.log'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv('ANALYZER_DB_PATH', 'history.db')
GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET', '')
GOOGLE_REDIRECT_URI = os.getenv('GOOGLE_REDIRECT_URI', 'https://tekoworld.com/auth/callback')
SESSION_SECRET = os.getenv('SESSION_SECRET', secrets.token_hex(32))

ALL_COINS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
    'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
    'ATOM/USDT', 'NEAR/USDT', 'APT/USDT', 'ARB/USDT', 'OP/USDT',
    'SUI/USDT', 'INJ/USDT', 'FET/USDT', 'TIA/USDT', 'SEI/USDT',
    'DOGE/USDT', 'PEPE/USDT', 'WIF/USDT', 'SHIB/USDT', 'LTC/USDT',
    'BCH/USDT', 'ETC/USDT', 'FIL/USDT', 'IMX/USDT', 'RENDER/USDT',
    'MAGIC/USDT', 'LIT/USDT', 'ZEN/USDT', 'ZEC/USDT', 'PUMP/USDT'
]


class HistoricalTracker:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _init_db(self):
        with self.get_connection() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS coin_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, date TEXT NOT NULL,
                symbol TEXT NOT NULL, price REAL NOT NULL, score INTEGER NOT NULL, grade TEXT NOT NULL,
                obv_trend TEXT, obv_divergence TEXT, funding_rate REAL, rsi REAL, volume_ratio REAL, signals TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, symbol TEXT NOT NULL,
                scan_count INTEGER NOT NULL, avg_score REAL NOT NULL, avg_grade TEXT NOT NULL,
                open_price REAL NOT NULL, close_price REAL NOT NULL, high_price REAL NOT NULL,
                low_price REAL NOT NULL, price_change_pct REAL NOT NULL, dominant_obv_trend TEXT,
                bullish_divergence_count INTEGER DEFAULT 0, bearish_divergence_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL, UNIQUE(date, symbol))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, google_id TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL, name TEXT, picture TEXT, created_at TEXT NOT NULL, last_login TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS user_coins (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, symbol TEXT NOT NULL,
                added_at TEXT NOT NULL, UNIQUE(user_id, symbol), FOREIGN KEY (user_id) REFERENCES users(id))""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_date ON coin_snapshots(symbol, date DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_summaries_symbol_date ON daily_summaries(symbol, date DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_coins_user ON user_coins(user_id)")
        logger.info(f"DB initialized: {self.db_path}")

    def get_or_create_user(self, google_id: str, email: str, name: str = None, picture: str = None) -> Dict:
        with self.get_connection() as conn:
            user = conn.execute("SELECT * FROM users WHERE google_id = ?", (google_id,)).fetchone()
            if user:
                conn.execute("UPDATE users SET last_login = ?, name = ?, picture = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), name, picture, user['id']))
                return dict(user)
            now = datetime.utcnow().isoformat()
            cursor = conn.execute("INSERT INTO users (google_id, email, name, picture, created_at, last_login) VALUES (?, ?, ?, ?, ?, ?)",
                (google_id, email, name, picture, now, now))
            user_id = cursor.lastrowid
            for symbol in ALL_COINS:
                conn.execute("INSERT OR IGNORE INTO user_coins (user_id, symbol, added_at) VALUES (?, ?, ?)", (user_id, symbol, now))
            return {'id': user_id, 'google_id': google_id, 'email': email, 'name': name, 'picture': picture, 'created_at': now}

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        with self.get_connection() as conn:
            user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            return dict(user) if user else None

    def get_user_coins(self, user_id: int) -> List[str]:
        with self.get_connection() as conn:
            return [r['symbol'] for r in conn.execute("SELECT symbol FROM user_coins WHERE user_id = ? ORDER BY added_at", (user_id,)).fetchall()]

    def add_user_coin(self, user_id: int, symbol: str) -> bool:
        symbol = symbol.upper() if '/' in symbol else symbol.upper() + '/USDT'
        with self.get_connection() as conn:
            try:
                conn.execute("INSERT OR IGNORE INTO user_coins (user_id, symbol, added_at) VALUES (?, ?, ?)", (user_id, symbol, datetime.utcnow().isoformat()))
                return True
            except: return False

    def remove_user_coin(self, user_id: int, symbol: str) -> bool:
        symbol = symbol.upper() if '/' in symbol else symbol.upper() + '/USDT'
        with self.get_connection() as conn:
            return conn.execute("DELETE FROM user_coins WHERE user_id = ? AND symbol = ?", (user_id, symbol)).rowcount > 0

    def get_all_watched_coins(self) -> List[str]:
        """Get all unique coins from all users' watchlists"""
        with self.get_connection() as conn:
            return [r['symbol'] for r in conn.execute("SELECT DISTINCT symbol FROM user_coins").fetchall()]

    def log_batch(self, coins: List[Dict]):
        if not coins: return
        snapshot_date = coins[0]['timestamp'][:10]
        with self.get_connection() as conn:
            conn.executemany("""INSERT INTO coin_snapshots (timestamp, date, symbol, price, score, grade, obv_trend, obv_divergence, funding_rate, rsi, volume_ratio, signals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(c['timestamp'], snapshot_date, c['symbol'], c['price'], c['score']['total_score'], c['score']['grade'],
                  c['indicators']['1h'].get('obv_trend'), c['indicators']['1h'].get('obv_divergence'), c['funding_rate'],
                  c['indicators']['1h'].get('rsi'), c['indicators']['1h'].get('volume_ratio'), json.dumps(c['signals'])) for c in coins])

    def aggregate_daily_summaries(self, target_date: Optional[str] = None):
        if target_date is None: target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            if conn.execute("SELECT COUNT(*) FROM daily_summaries WHERE date = ?", (target_date,)).fetchone()[0] > 0: return
            for (symbol,) in conn.execute("SELECT DISTINCT symbol FROM coin_snapshots WHERE date = ?", (target_date,)).fetchall():
                rows = conn.execute("SELECT price, score, obv_trend, obv_divergence FROM coin_snapshots WHERE symbol = ? AND date = ? ORDER BY timestamp", (symbol, target_date)).fetchall()
                if not rows: continue
                prices, scores = [r['price'] for r in rows], [r['score'] for r in rows]
                avg_score = sum(scores) / len(scores)
                conn.execute("""INSERT OR REPLACE INTO daily_summaries (date, symbol, scan_count, avg_score, avg_grade, open_price, close_price, high_price, low_price, price_change_pct, dominant_obv_trend, bullish_divergence_count, bearish_divergence_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (target_date, symbol, len(rows), round(avg_score, 1), 'A+' if avg_score >= 90 else 'A' if avg_score >= 80 else 'B' if avg_score >= 70 else 'C' if avg_score >= 60 else 'D' if avg_score >= 50 else 'F',
                     prices[0], prices[-1], max(prices), min(prices), round((prices[-1] - prices[0]) / prices[0] * 100, 2) if prices[0] > 0 else 0,
                     'neutral', sum(1 for r in rows if r['obv_divergence'] == 'bullish'), sum(1 for r in rows if r['obv_divergence'] == 'bearish'), datetime.utcnow().isoformat()))
            logger.info(f"Aggregated for {target_date}")

    def get_coin_daily_history(self, symbol: str, limit: int = 30, before_date: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
        with self.get_connection() as conn:
            q = "SELECT * FROM daily_summaries WHERE symbol = ?" + (" AND date < ?" if before_date else "") + " ORDER BY date DESC LIMIT ?"
            rows = conn.execute(q, (symbol, before_date, limit + 1) if before_date else (symbol, limit + 1)).fetchall()
            has_more = len(rows) > limit
            return [dict(r) for r in rows[:limit]], (rows[-1]['date'] if has_more and rows else None)

    def get_today_stats(self, symbol: str) -> Optional[Dict]:
        today = datetime.utcnow().strftime('%Y-%m-%d')
        with self.get_connection() as conn:
            rows = conn.execute("SELECT price, score FROM coin_snapshots WHERE symbol = ? AND date = ? ORDER BY timestamp", (symbol, today)).fetchall()
            if not rows: return None
            prices, scores = [r['price'] for r in rows], [r['score'] for r in rows]
            avg = sum(scores) / len(scores)
            return {'date': today, 'avg_score': round(avg, 1), 'avg_grade': 'A+' if avg >= 90 else 'A' if avg >= 80 else 'B' if avg >= 70 else 'C' if avg >= 60 else 'D' if avg >= 50 else 'F',
                    'open_price': prices[0], 'current_price': prices[-1], 'price_change_pct': round((prices[-1] - prices[0]) / prices[0] * 100, 2) if prices[0] > 0 else 0, 'scan_count': len(rows), 'is_today': True}

    def cleanup_old_snapshots(self, keep_days: int = 90):
        with self.get_connection() as conn:
            r = conn.execute("DELETE FROM coin_snapshots WHERE date < ?", ((datetime.utcnow() - timedelta(days=keep_days)).strftime('%Y-%m-%d'),))
            if r.rowcount > 0: logger.info(f"Cleaned {r.rowcount} old snapshots"); conn.execute("VACUUM")


class MarketAnalyzer:
    def __init__(self):
        self.exchange = self._init_exchange()
        self.spot_exchange = self._init_spot_exchange()
        self.coins = ALL_COINS.copy()
        self.weights = {'volume': 20, 'obv': 35, 'funding': 20, 'rsi': 25}
        self._normalize_weights()
        self.obv_lookback = 14
        self.rsi_period = 14
        self.cache = {}
        self.cache_duration = 60
        logger.info(f"Analyzer initialized with {len(self.coins)} coins")

    def _normalize_weights(self):
        total = sum(self.weights.values())
        if total != 100: self.weights = {k: round(v * 100 / total, 1) for k, v in self.weights.items()}

    def update_weights(self, new_weights: Dict[str, float]):
        for k in ['volume', 'obv', 'funding', 'rsi']:
            if k in new_weights: self.weights[k] = new_weights[k]
        self._normalize_weights()

    def _init_exchange(self) -> ccxt.Exchange:
        ex = ccxt.binance({'apiKey': os.getenv('BINANCE_API_KEY', ''), 'secret': os.getenv('BINANCE_SECRET_KEY', ''),
            'enableRateLimit': True, 'options': {'defaultType': 'future', 'adjustForTimeDifference': True}})
        try: ex.load_markets(); logger.info(f"Connected to Binance Futures")
        except Exception as e: logger.error(f"Binance error: {e}")
        return ex

    def _init_spot_exchange(self) -> ccxt.Exchange:
        ex = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
        try: ex.load_markets(); logger.info("Connected to Binance Spot")
        except Exception as e: logger.error(f"Spot error: {e}")
        return ex

    async def get_ohlcv(self, symbol: str, timeframe: str = '1h', limit: int = 200) -> pd.DataFrame:
        cache_key = f"{symbol}_{timeframe}_{limit}"
        if cache_key in self.cache and time.time() - self.cache[cache_key]['time'] < self.cache_duration:
            return self.cache[cache_key]['data'].copy()
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 50: return pd.DataFrame()
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            self.cache[cache_key] = {'data': df.copy(), 'time': time.time()}
            return df
        except: return pd.DataFrame()

    async def get_funding_rate(self, symbol: str) -> float:
        try: return self.exchange.fetch_funding_rate(symbol).get('fundingRate', 0) or 0
        except: return 0

    async def get_spot_volume(self, symbol: str) -> Dict:
        try:
            ohlcv = self.spot_exchange.fetch_ohlcv(symbol, '1h', limit=48)
            if ohlcv and len(ohlcv) >= 48:
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['vol_usdt'] = df['volume'] * df['close']
                v24, vp24 = float(df['vol_usdt'].iloc[-24:].sum()), float(df['vol_usdt'].iloc[-48:-24].sum())
                return {'volume': v24, 'change': round((v24 - vp24) / vp24 * 100, 1) if vp24 > 0 else 0}
            return {'volume': 0, 'change': 0}
        except: return {'volume': 0, 'change': 0}

    def calculate_indicators(self, df: pd.DataFrame) -> Dict:
        if df.empty or len(df) < 50: return {}
        try:
            close, high, low, volume = df['close'], df['high'], df['low'], df['volume']
            price = float(close.iloc[-1])
            atr = ta_lib.volatility.average_true_range(high, low, close, window=14)
            atr_val = float(atr.iloc[-1]) if atr is not None and not pd.isna(atr.iloc[-1]) else 0
            atr_pct = (atr_val / price * 100) if price > 0 else 2.0
            div_thresh = max(1.5, min(atr_pct * 1.5, 5.0))
            
            if len(volume) >= 48:
                vol_ratio = float(volume.iloc[-25:-1].sum()) / float(volume.iloc[-49:-25].sum()) if float(volume.iloc[-49:-25].sum()) > 0 else 1.0
            else:
                vol_ratio = float(volume.iloc[-2]) / float(volume.iloc[-20:-1].mean()) if float(volume.iloc[-20:-1].mean()) > 0 else 1.0

            obv = ta_lib.volume.on_balance_volume(close, volume)
            if obv is not None and len(obv) >= self.obv_lookback:
                obv_cur, obv_prev = float(obv.iloc[-1]), float(obv.iloc[-self.obv_lookback])
                obv_chg = ((obv_cur - obv_prev) / abs(obv_prev) * 100) if obv_prev != 0 else 0
                obv_slope = np.polyfit(range(14), obv.tail(14).values, 1)[0]
                obv_slope_n = obv_slope / abs(obv_cur) * 1000 if obv_cur != 0 else 0
                obv_trend = 'bullish' if obv_slope_n > 0.5 else 'bearish' if obv_slope_n < -0.5 else 'neutral'
                obv_strength = 'strong' if abs(obv_slope_n) > 1.5 else 'moderate' if abs(obv_slope_n) > 0.5 else 'weak'
                price_chg = ((price - float(close.iloc[-14])) / float(close.iloc[-14]) * 100)
                obv_div = 'bullish' if price_chg < -div_thresh and obv_chg > div_thresh else 'bearish' if price_chg > div_thresh and obv_chg < -div_thresh else 'none'
            else:
                obv_cur, obv_chg, obv_trend, obv_strength, obv_div = 0, 0, 'neutral', 'weak', 'none'

            rsi = ta_lib.momentum.rsi(close, window=self.rsi_period)
            rsi_val = float(rsi.iloc[-1]) if rsi is not None and not pd.isna(rsi.iloc[-1]) else 50

            sma20 = ta_lib.trend.sma_indicator(close, window=20)
            sma50 = ta_lib.trend.sma_indicator(close, window=50) if len(close) >= 50 else None
            sma20_v = float(sma20.iloc[-1]) if sma20 is not None and not pd.isna(sma20.iloc[-1]) else price
            sma50_v = float(sma50.iloc[-1]) if sma50 is not None and not pd.isna(sma50.iloc[-1]) else sma20_v
            trend = 'uptrend' if price > sma20_v > sma50_v else 'downtrend' if price < sma20_v < sma50_v else 'sideways'
            price_chg_24h = ((price - float(close.iloc[-24])) / float(close.iloc[-24]) * 100) if len(close) >= 24 else 0

            macd_l, macd_s = ta_lib.trend.macd(close), ta_lib.trend.macd_signal(close)
            macd_bull = float(macd_l.iloc[-1]) > float(macd_s.iloc[-1]) if macd_l is not None and macd_s is not None and not pd.isna(macd_l.iloc[-1]) and not pd.isna(macd_s.iloc[-1]) else False

            return {'price': round(price, 6), 'price_change_24h': round(price_chg_24h, 2), 'volume_ratio': round(vol_ratio, 2),
                    'obv': round(obv_cur, 2), 'obv_change': round(obv_chg, 2), 'obv_trend': obv_trend, 'obv_strength': obv_strength,
                    'obv_divergence': obv_div, 'divergence_threshold': round(div_thresh, 2), 'rsi': round(rsi_val, 2),
                    'sma_20': round(sma20_v, 6), 'sma_50': round(sma50_v, 6), 'trend': trend, 'atr_percent': round(atr_pct, 2), 'macd_bullish': macd_bull,
                    'atr': round(atr_val, 6)}
        except Exception as e:
            logger.error(f"Indicator error: {e}")
            return {}

    def calculate_levels(self, df: pd.DataFrame, current_price: float) -> Dict:
        """Calculate support/resistance levels using volume profile and swing points"""
        if df.empty or len(df) < 50:
            return {'supports': [], 'resistances': [], 'volume_profile': []}

        try:
            high, low, close, volume = df['high'].values, df['low'].values, df['close'].values, df['volume'].values

            # Calculate ATR for level strength threshold
            atr = float(ta_lib.volatility.average_true_range(df['high'], df['low'], df['close'], window=14).iloc[-1])
            atr_pct = atr / current_price if current_price > 0 else 0.02

            # 1. Find Swing Highs and Lows (local extrema)
            swing_highs, swing_lows = [], []
            lookback = 5  # candles on each side to confirm swing

            for i in range(lookback, len(df) - lookback):
                # Swing High: highest point in the window
                if high[i] == max(high[i-lookback:i+lookback+1]):
                    swing_highs.append({'price': float(high[i]), 'index': i, 'volume': float(volume[i])})
                # Swing Low: lowest point in the window
                if low[i] == min(low[i-lookback:i+lookback+1]):
                    swing_lows.append({'price': float(low[i]), 'index': i, 'volume': float(volume[i])})

            # 2. Volume Profile Analysis - find high volume price zones
            price_min, price_max = float(low.min()), float(high.max())
            num_bins = 20
            bin_size = (price_max - price_min) / num_bins if price_max > price_min else price_max * 0.01

            volume_profile = []
            for i in range(num_bins):
                bin_low = price_min + i * bin_size
                bin_high = bin_low + bin_size
                bin_mid = (bin_low + bin_high) / 2

                # Sum volume for candles that traded in this price range
                bin_volume = 0
                for j in range(len(df)):
                    if low[j] <= bin_high and high[j] >= bin_low:
                        # Proportional volume based on overlap
                        candle_range = high[j] - low[j] if high[j] > low[j] else 0.0001
                        overlap = min(high[j], bin_high) - max(low[j], bin_low)
                        if overlap > 0:
                            bin_volume += volume[j] * (overlap / candle_range)

                volume_profile.append({'price': round(bin_mid, 6), 'volume': round(bin_volume, 2), 'low': round(bin_low, 6), 'high': round(bin_high, 6)})

            # Find Point of Control (POC) - highest volume level
            poc = max(volume_profile, key=lambda x: x['volume']) if volume_profile else None

            # 3. Identify High Volume Nodes (HVN) - top 30% volume levels
            sorted_by_vol = sorted(volume_profile, key=lambda x: x['volume'], reverse=True)
            hvn_threshold = len(sorted_by_vol) * 0.3
            hvn_levels = sorted_by_vol[:int(hvn_threshold)]

            # 4. Cluster nearby levels and calculate strength
            def cluster_levels(levels: List[Dict], threshold: float) -> List[Dict]:
                if not levels:
                    return []
                sorted_levels = sorted(levels, key=lambda x: x['price'])
                clusters = []
                current_cluster = [sorted_levels[0]]

                for level in sorted_levels[1:]:
                    if abs(level['price'] - current_cluster[-1]['price']) / current_price < threshold:
                        current_cluster.append(level)
                    else:
                        # Finalize cluster
                        avg_price = sum(l['price'] for l in current_cluster) / len(current_cluster)
                        total_vol = sum(l.get('volume', 0) for l in current_cluster)
                        clusters.append({'price': avg_price, 'volume': total_vol, 'touches': len(current_cluster)})
                        current_cluster = [level]

                # Don't forget last cluster
                if current_cluster:
                    avg_price = sum(l['price'] for l in current_cluster) / len(current_cluster)
                    total_vol = sum(l.get('volume', 0) for l in current_cluster)
                    clusters.append({'price': avg_price, 'volume': total_vol, 'touches': len(current_cluster)})

                return clusters

            # Cluster swing levels (2% threshold)
            support_clusters = cluster_levels(swing_lows, 0.02)
            resistance_clusters = cluster_levels(swing_highs, 0.02)

            # 5. Combine with volume profile HVN levels
            for hvn in hvn_levels:
                if hvn['price'] < current_price:
                    # Check if close to existing support
                    merged = False
                    for sup in support_clusters:
                        if abs(sup['price'] - hvn['price']) / current_price < 0.015:
                            sup['volume'] += hvn['volume']
                            sup['hvn'] = True
                            merged = True
                            break
                    if not merged:
                        support_clusters.append({'price': hvn['price'], 'volume': hvn['volume'], 'touches': 1, 'hvn': True})
                else:
                    # Check if close to existing resistance
                    merged = False
                    for res in resistance_clusters:
                        if abs(res['price'] - hvn['price']) / current_price < 0.015:
                            res['volume'] += hvn['volume']
                            res['hvn'] = True
                            merged = True
                            break
                    if not merged:
                        resistance_clusters.append({'price': hvn['price'], 'volume': hvn['volume'], 'touches': 1, 'hvn': True})

            # 6. Calculate strength score for each level
            max_vol = max([l['volume'] for l in support_clusters + resistance_clusters]) if support_clusters or resistance_clusters else 1

            def calc_strength(level: Dict, is_support: bool) -> Dict:
                vol_score = (level['volume'] / max_vol) * 40 if max_vol > 0 else 0
                touch_score = min(level['touches'] * 15, 30)
                # Distance from current price (closer = more relevant)
                dist_pct = abs(level['price'] - current_price) / current_price
                dist_score = max(0, 30 - dist_pct * 100)

                strength = vol_score + touch_score + dist_score
                strength = min(100, max(0, strength))

                # Calculate distance percentage
                distance_pct = ((current_price - level['price']) / current_price * 100) if is_support else ((level['price'] - current_price) / current_price * 100)

                return {
                    'price': round(level['price'], 6),
                    'strength': round(strength),
                    'type': 'demand' if is_support else 'supply',
                    'distance_pct': round(distance_pct, 2),
                    'touches': level.get('touches', 1),
                    'hvn': level.get('hvn', False)
                }

            # Filter and sort supports (below current price)
            supports = [calc_strength(s, True) for s in support_clusters if s['price'] < current_price * 0.995]
            supports = sorted(supports, key=lambda x: x['price'], reverse=True)[:4]  # Top 4 closest supports

            # Filter and sort resistances (above current price)
            resistances = [calc_strength(r, False) for r in resistance_clusters if r['price'] > current_price * 1.005]
            resistances = sorted(resistances, key=lambda x: x['price'])[:4]  # Top 4 closest resistances

            # Add SMA levels as dynamic support/resistance
            sma20 = float(ta_lib.trend.sma_indicator(df['close'], window=20).iloc[-1])
            sma50 = float(ta_lib.trend.sma_indicator(df['close'], window=50).iloc[-1]) if len(df) >= 50 else None

            dynamic_levels = []
            if sma20 and abs(sma20 - current_price) / current_price > 0.005:
                dynamic_levels.append({
                    'price': round(sma20, 6),
                    'type': 'sma20',
                    'label': 'SMA 20',
                    'is_support': sma20 < current_price
                })
            if sma50 and abs(sma50 - current_price) / current_price > 0.005:
                dynamic_levels.append({
                    'price': round(sma50, 6),
                    'type': 'sma50',
                    'label': 'SMA 50',
                    'is_support': sma50 < current_price
                })

            # Point of Control
            poc_data = None
            if poc:
                poc_data = {
                    'price': round(poc['price'], 6),
                    'is_support': poc['price'] < current_price,
                    'distance_pct': round(abs(poc['price'] - current_price) / current_price * 100, 2)
                }

            return {
                'supports': supports,
                'resistances': resistances,
                'poc': poc_data,
                'dynamic_levels': dynamic_levels,
                'atr': round(atr, 6),
                'atr_pct': round(atr_pct * 100, 2)
            }

        except Exception as e:
            logger.error(f"Levels calculation error: {e}")
            return {'supports': [], 'resistances': [], 'poc': None, 'dynamic_levels': [], 'atr': 0, 'atr_pct': 0}

    def score_coin(self, ind: Dict, funding: float) -> Dict:
        scores, details = {}, []
        vr = ind.get('volume_ratio', 1.0)
        scores['volume'] = 100 if vr >= 1.5 else 80 if vr >= 1.1 else 60 if vr >= 0.8 else 40
        
        ot, os, od = ind.get('obv_trend', 'neutral'), ind.get('obv_strength', 'weak'), ind.get('obv_divergence', 'none')
        scores['obv'] = 100 if ot == 'bullish' and os == 'strong' else 80 if ot == 'bullish' else 20 if ot == 'bearish' and os == 'strong' else 40 if ot == 'bearish' else 60
        if od == 'bullish': scores['obv'] = min(100, scores['obv'] + 15)
        elif od == 'bearish': scores['obv'] = max(0, scores['obv'] - 15)

        fp = funding * 100
        scores['funding'] = 100 if -0.005 <= fp <= 0.005 else 80 if -0.01 <= fp <= 0.01 else 60 if -0.02 <= fp <= 0.02 else 20
        
        rsi = ind.get('rsi', 50)
        scores['rsi'] = 100 if 40 <= rsi <= 60 else 80 if 30 <= rsi <= 70 else 40 if 20 <= rsi <= 80 else 20

        total = sum(scores[k] * (self.weights[k] / 100) for k in scores)
        grade = 'A+' if total >= 90 else 'A' if total >= 80 else 'B' if total >= 70 else 'C' if total >= 60 else 'D' if total >= 50 else 'F'
        return {'total_score': round(total), 'percentage': round(total, 1), 'component_scores': scores, 'grade': grade, 'weights_used': self.weights.copy()}

    async def analyze_coin(self, symbol: str) -> Optional[Dict]:
        try:
            df_1h, df_4h = await self.get_ohlcv(symbol, '1h', 200), await self.get_ohlcv(symbol, '4h', 100)
            if df_1h.empty: return None
            ind_1h = self.calculate_indicators(df_1h)
            ind_4h = self.calculate_indicators(df_4h) if not df_4h.empty else {}
            if not ind_1h: return None
            funding = await self.get_funding_rate(symbol)
            spot = await self.get_spot_volume(symbol)
            price = ind_1h.get('price', 1)
            fv = float(df_1h['volume'].iloc[-25:-1].sum()) * price
            fvp = float(df_1h['volume'].iloc[-49:-25].sum()) * price if len(df_1h) >= 49 else fv
            fvc = ((fv - fvp) / fvp * 100) if fvp > 0 else 0
            score = self.score_coin(ind_1h, funding)
            signals = self._gen_signals(ind_1h, ind_4h, funding)
            # Calculate support/resistance levels
            levels = self.calculate_levels(df_1h, price)
            # Calculate momentum analysis for trend insights
            momentum = self._analyze_momentum(ind_1h, ind_4h, funding)
            return {'symbol': symbol, 'display_name': symbol.replace('/USDT', ''), 'timestamp': datetime.utcnow().isoformat(),
                    'price': ind_1h.get('price', 0), 'price_change_24h': ind_1h.get('price_change_24h', 0),
                    'indicators': {'1h': ind_1h, '4h': ind_4h}, 'funding_rate': round(funding * 100, 4),
                    'spot_volume': round(spot['volume'], 0), 'spot_volume_change': round(spot['change'], 1),
                    'futures_volume': round(fv, 0), 'futures_volume_change': round(fvc, 1), 'score': score, 'signals': signals,
                    'levels': levels, 'momentum': momentum}
        except Exception as e:
            logger.debug(f"Analyze error {symbol}: {e}")
            return None

    def _gen_signals(self, i1: Dict, i4: Dict, f: float) -> List[str]:
        s = []
        vr = i1.get('volume_ratio', 1.0)
        if vr >= 1.5: s.append("[VOL] High 24h activity")
        elif vr <= 0.6: s.append("[VOL] Low 24h activity")
        ot, os, od = i1.get('obv_trend', 'neutral'), i1.get('obv_strength', 'weak'), i1.get('obv_divergence', 'none')
        if ot == 'bullish': s.append("[OBV] Strong accumulation" if os == 'strong' else "[OBV] Accumulation")
        elif ot == 'bearish': s.append("[OBV] Strong distribution" if os == 'strong' else "[OBV] Distribution")
        if od == 'bullish': s.append("[OBV] Bullish divergence!")
        elif od == 'bearish': s.append("[OBV] Bearish divergence!")
        rsi = i1.get('rsi', 50)
        if rsi < 30: s.append("[RSI] Oversold")
        elif rsi > 70: s.append("[RSI] Overbought")
        if f * 100 > 0.03: s.append("[FUND] Longs paying")
        elif f * 100 < -0.03: s.append("[FUND] Shorts paying")
        t1, t4 = i1.get('trend', 'sideways'), i4.get('trend', 'sideways') if i4 else 'sideways'
        if t1 == 'uptrend' and t4 == 'uptrend': s.append("[TREND] Bullish alignment")
        elif t1 == 'downtrend' and t4 == 'downtrend': s.append("[TREND] Bearish alignment")
        if i1.get('macd_bullish'): s.append("[MACD] Bullish")
        return s if s else ["No signals"]

    def _analyze_momentum(self, i1: Dict, i4: Dict, funding: float) -> Dict:
        """
        Analyze momentum across timeframes to generate human-readable trend insights.
        Returns text describing short-term (24-48h) vs higher timeframe outlook.
        """
        # Extract indicators
        trend_1h = i1.get('trend', 'sideways')
        trend_4h = i4.get('trend', 'sideways') if i4 else 'sideways'
        obv_trend = i1.get('obv_trend', 'neutral')
        obv_strength = i1.get('obv_strength', 'weak')
        obv_div = i1.get('obv_divergence', 'none')
        obv_4h = i4.get('obv_trend', 'neutral') if i4 else 'neutral'
        rsi = i1.get('rsi', 50)
        rsi_4h = i4.get('rsi', 50) if i4 else 50
        macd_bull = i1.get('macd_bullish', False)
        macd_4h_bull = i4.get('macd_bullish', False) if i4 else False
        vol_ratio = i1.get('volume_ratio', 1.0)
        funding_pct = funding * 100

        # Calculate bullish/bearish scores for each timeframe
        # Lower timeframe (1h) - short-term outlook
        ltf_bull_score = 0
        ltf_bear_score = 0

        if trend_1h == 'uptrend': ltf_bull_score += 2
        elif trend_1h == 'downtrend': ltf_bear_score += 2

        if obv_trend == 'bullish': ltf_bull_score += 2 if obv_strength == 'strong' else 1
        elif obv_trend == 'bearish': ltf_bear_score += 2 if obv_strength == 'strong' else 1

        if macd_bull: ltf_bull_score += 1
        else: ltf_bear_score += 1

        if rsi < 30: ltf_bull_score += 1  # Oversold = potential bounce
        elif rsi > 70: ltf_bear_score += 1  # Overbought = potential pullback
        elif rsi < 45: ltf_bear_score += 0.5
        elif rsi > 55: ltf_bull_score += 0.5

        # Higher timeframe (4h) - macro outlook
        htf_bull_score = 0
        htf_bear_score = 0

        if trend_4h == 'uptrend': htf_bull_score += 2
        elif trend_4h == 'downtrend': htf_bear_score += 2

        if obv_4h == 'bullish': htf_bull_score += 1.5
        elif obv_4h == 'bearish': htf_bear_score += 1.5

        if macd_4h_bull: htf_bull_score += 1
        else: htf_bear_score += 1

        if rsi_4h < 35: htf_bull_score += 1
        elif rsi_4h > 65: htf_bear_score += 1

        # Determine bias for each timeframe
        ltf_bias = 'bullish' if ltf_bull_score > ltf_bear_score + 1 else 'bearish' if ltf_bear_score > ltf_bull_score + 1 else 'neutral'
        htf_bias = 'bullish' if htf_bull_score > htf_bear_score + 1 else 'bearish' if htf_bear_score > htf_bull_score + 1 else 'neutral'

        # Volume confirmation
        vol_confirms = vol_ratio >= 1.2
        vol_weak = vol_ratio <= 0.7

        # Generate insight text based on conditions
        text = ""
        sentiment = "neutral"  # bullish, bearish, neutral, caution
        confidence = "medium"  # high, medium, low

        # === STRONG CONTINUATION SCENARIOS ===
        if htf_bias == 'bullish' and ltf_bias == 'bullish':
            if obv_strength == 'strong' and vol_confirms:
                text = "Strong bullish momentum. Trend likely to continue across all timeframes."
                sentiment = "bullish"
                confidence = "high"
            elif obv_strength == 'strong':
                text = "Bullish trend intact. Both timeframes aligned upward."
                sentiment = "bullish"
                confidence = "high"
            else:
                text = "Uptrend on both timeframes. Momentum supports continuation."
                sentiment = "bullish"
                confidence = "medium"

        elif htf_bias == 'bearish' and ltf_bias == 'bearish':
            if obv_strength == 'strong' and vol_confirms:
                text = "Strong bearish momentum. Downtrend likely to continue across all timeframes."
                sentiment = "bearish"
                confidence = "high"
            elif obv_strength == 'strong':
                text = "Bearish trend intact. Both timeframes aligned downward."
                sentiment = "bearish"
                confidence = "high"
            else:
                text = "Downtrend on both timeframes. Selling pressure persists."
                sentiment = "bearish"
                confidence = "medium"

        # === DIVERGENCE / REVERSAL SCENARIOS ===
        elif obv_div == 'bullish':
            if htf_bias == 'bearish':
                text = "Accumulation detected. Local bottom may be forming despite bearish price action."
                sentiment = "caution"
                confidence = "medium"
            else:
                text = "Hidden accumulation underway. Smart money buying despite price weakness."
                sentiment = "bullish"
                confidence = "medium"

        elif obv_div == 'bearish':
            if htf_bias == 'bullish':
                text = "Distribution detected. Local top may be forming despite bullish price action."
                sentiment = "caution"
                confidence = "medium"
            else:
                text = "Hidden distribution underway. Smart money selling despite price strength."
                sentiment = "bearish"
                confidence = "medium"

        # === TIMEFRAME CONFLICT SCENARIOS ===
        elif htf_bias == 'bullish' and ltf_bias == 'bearish':
            if rsi < 35:
                text = "Short-term pullback in a bullish market. Potential dip-buying opportunity."
                sentiment = "caution"
                confidence = "medium"
            elif vol_weak:
                text = "Minor correction on low volume. Higher timeframe still bullish."
                sentiment = "bullish"
                confidence = "medium"
            else:
                text = "Short-term weakness, but macro trend remains bullish. Watch for bounce."
                sentiment = "caution"
                confidence = "medium"

        elif htf_bias == 'bearish' and ltf_bias == 'bullish':
            if rsi > 65:
                text = "Relief rally in a bearish market. Potential bull trap - use caution."
                sentiment = "caution"
                confidence = "medium"
            elif vol_weak:
                text = "Low-volume bounce. Higher timeframe still bearish."
                sentiment = "bearish"
                confidence = "medium"
            else:
                text = "Short-term strength, but macro trend remains bearish. Watch for rejection."
                sentiment = "caution"
                confidence = "medium"

        # === NEUTRAL / CONSOLIDATION SCENARIOS ===
        elif htf_bias == 'neutral' and ltf_bias == 'neutral':
            if vol_weak:
                text = "Low volatility consolidation. Waiting for a breakout signal."
                sentiment = "neutral"
                confidence = "low"
            else:
                text = "Ranging market. No clear trend direction on either timeframe."
                sentiment = "neutral"
                confidence = "low"

        elif htf_bias == 'bullish' and ltf_bias == 'neutral':
            text = "Macro bullish, short-term consolidating. Likely to continue up after pause."
            sentiment = "bullish"
            confidence = "medium"

        elif htf_bias == 'bearish' and ltf_bias == 'neutral':
            text = "Macro bearish, short-term consolidating. Likely to continue down after pause."
            sentiment = "bearish"
            confidence = "medium"

        elif htf_bias == 'neutral' and ltf_bias == 'bullish':
            text = "Short-term bullish momentum building. Watch for higher timeframe confirmation."
            sentiment = "caution"
            confidence = "low"

        elif htf_bias == 'neutral' and ltf_bias == 'bearish':
            text = "Short-term bearish pressure. Watch for higher timeframe confirmation."
            sentiment = "caution"
            confidence = "low"

        # === EXTREME RSI CONDITIONS (override) ===
        if rsi < 25 and htf_bias != 'bearish':
            text = "Extremely oversold. Bounce likely in the next 24-48 hours."
            sentiment = "bullish"
            confidence = "high"
        elif rsi > 75 and htf_bias != 'bullish':
            text = "Extremely overbought. Pullback likely in the next 24-48 hours."
            sentiment = "bearish"
            confidence = "high"

        # === FUNDING RATE EXTREMES (additional context) ===
        if funding_pct > 0.05 and sentiment == 'bullish':
            text += " Caution: High funding suggests crowded longs."
            confidence = "medium" if confidence == "high" else confidence
        elif funding_pct < -0.05 and sentiment == 'bearish':
            text += " Caution: Negative funding suggests crowded shorts."
            confidence = "medium" if confidence == "high" else confidence

        # Fallback
        if not text:
            text = "Mixed signals. No clear short-term direction."
            sentiment = "neutral"
            confidence = "low"

        return {
            'text': text,
            'sentiment': sentiment,
            'confidence': confidence,
            'ltf_bias': ltf_bias,
            'htf_bias': htf_bias
        }

    async def scan_all(self) -> List[Dict]:
        results = []
        for sym in self.coins:
            try:
                r = await self.analyze_coin(sym)
                if r: results.append(r)
                await asyncio.sleep(0.15)
            except: pass
        return sorted(results, key=lambda x: x['score']['total_score'], reverse=True)


class AnalyzerApp:
    def __init__(self):
        self.analyzer = MarketAnalyzer()
        self.tracker = HistoricalTracker()
        self.app = FastAPI(title="Crypto Market Analyzer", version="3.0.0")
        self.app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
        self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
        self.running = False
        self.scan_results = []
        self.last_scan = None
        self.scan_count = 0
        self.scan_interval = 300
        self.websockets: Set[WebSocket] = set()
        self._setup_routes()
        logger.info("App initialized")

    def _get_user(self, request: Request) -> Optional[Dict]:
        uid = request.session.get('user_id')
        return self.tracker.get_user_by_id(uid) if uid else None

    def _require_auth(self, request: Request) -> Dict:
        user = self._get_user(request)
        if not user: raise HTTPException(status_code=401, detail="Not authenticated")
        return user

    def _setup_routes(self):
        @self.app.get("/auth/login")
        async def login():
            url = f"https://accounts.google.com/o/oauth2/v2/auth?client_id={GOOGLE_CLIENT_ID}&redirect_uri={GOOGLE_REDIRECT_URI}&response_type=code&scope=openid%20email%20profile&access_type=offline&prompt=consent"
            return RedirectResponse(url=url)

        @self.app.get("/auth/callback")
        async def callback(request: Request, code: str = None, error: str = None):
            if error or not code: return RedirectResponse(url="/?error=auth_failed")
            try:
                async with httpx.AsyncClient() as client:
                    tr = await client.post("https://oauth2.googleapis.com/token", data={
                        'client_id': GOOGLE_CLIENT_ID, 'client_secret': GOOGLE_CLIENT_SECRET,
                        'code': code, 'grant_type': 'authorization_code', 'redirect_uri': GOOGLE_REDIRECT_URI})
                    tokens = tr.json()
                    if 'error' in tokens: return RedirectResponse(url="/?error=token_failed")
                    ui = await client.get("https://www.googleapis.com/oauth2/v2/userinfo", headers={'Authorization': f"Bearer {tokens['access_token']}"})
                    userinfo = ui.json()
                user = self.tracker.get_or_create_user(userinfo['id'], userinfo['email'], userinfo.get('name'), userinfo.get('picture'))
                request.session['user_id'] = user['id']
                return RedirectResponse(url="/")
            except Exception as e:
                logger.error(f"Auth error: {e}")
                return RedirectResponse(url="/?error=auth_error")

        @self.app.get("/auth/logout")
        async def logout(request: Request):
            request.session.clear()
            return RedirectResponse(url="/")

        @self.app.get("/api/me")
        async def me(request: Request):
            user = self._get_user(request)
            if not user: return JSONResponse({'authenticated': False})
            return JSONResponse({'authenticated': True, 'user': {'id': user['id'], 'email': user['email'], 'name': user['name'], 'picture': user['picture']}, 'coins': self.tracker.get_user_coins(user['id'])})

        @self.app.post("/api/coins/add")
        async def add_coin(request: Request):
            user = self._require_auth(request)
            body = await request.json()
            sym = body.get('symbol', '').upper()
            sym = sym if '/' in sym else sym + '/USDT'
            # Validate against Binance futures markets
            if sym not in self.analyzer.exchange.markets:
                return JSONResponse({'error': 'Invalid coin - not available on Binance Futures'}, status_code=400)
            return JSONResponse({'success': self.tracker.add_user_coin(user['id'], sym), 'symbol': sym})

        @self.app.post("/api/coins/remove")
        async def remove_coin(request: Request):
            user = self._require_auth(request)
            body = await request.json()
            sym = body.get('symbol', '').upper()
            sym = sym if '/' in sym else sym + '/USDT'
            return JSONResponse({'success': self.tracker.remove_user_coin(user['id'], sym), 'symbol': sym})

        @self.app.get("/api/coins/available")
        async def available(): return JSONResponse({'coins': ALL_COINS})

        @self.app.get("/api/coins/search")
        async def search_coins(request: Request, q: str = Query("", min_length=1)):
            if not self._get_user(request): return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            q = q.upper()
            user = self._get_user(request)
            user_coins = self.tracker.get_user_coins(user['id']) if user else []
            # Search in Binance futures markets for USDT pairs
            results = []
            for symbol, market in self.analyzer.exchange.markets.items():
                if '/USDT' in symbol and q in symbol:
                    base = symbol.replace('/USDT', '')
                    results.append({
                        'symbol': symbol,
                        'base': base,
                        'inWatchlist': symbol in user_coins,
                        'inDefault': symbol in ALL_COINS
                    })
                    if len(results) >= 50: break  # Limit results
            # Sort: exact matches first, then alphabetically
            results.sort(key=lambda x: (0 if x['base'] == q else 1, x['base']))
            return JSONResponse({'results': results[:30]})

        @self.app.get("/")
        async def dashboard(request: Request):
            user = self._get_user(request)
            return HTMLResponse(self._get_dashboard_html() if user else self._get_login_html())

        @self.app.get("/api/scan")
        async def scan(request: Request):
            user = self._get_user(request)
            if not user: return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            uc = self.tracker.get_user_coins(user['id'])
            return JSONResponse({'results': [r for r in self.scan_results if r['symbol'] in uc],
                'last_scan': self.last_scan.isoformat() if self.last_scan else None, 'scan_count': self.scan_count, 'weights': self.analyzer.weights, 'user_coins': uc})

        @self.app.get("/api/coin/{symbol}")
        async def coin(request: Request, symbol: str):
            if not self._get_user(request): return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            sym = symbol.upper() if '/' in symbol.upper() else symbol.upper() + '/USDT'
            r = await self.analyzer.analyze_coin(sym)
            return JSONResponse(r or {'error': 'Not found'})

        @self.app.get("/api/coin/{symbol}/history")
        async def history(request: Request, symbol: str, limit: int = Query(30, ge=1, le=100), before: str = Query(None)):
            if not self._get_user(request): return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            sym = symbol.upper() if '/' in symbol.upper() else symbol.upper() + '/USDT'
            today = self.tracker.get_today_stats(sym)
            hist, cursor = self.tracker.get_coin_daily_history(sym, limit, before)
            return JSONResponse({'symbol': sym, 'today': today, 'history': hist, 'next_cursor': cursor, 'has_more': cursor is not None})

        @self.app.post("/api/scan/trigger")
        async def trigger(request: Request):
            if not self._get_user(request): return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            asyncio.create_task(self._run_scan())
            return JSONResponse({'status': 'triggered'})

        @self.app.post("/api/weights")
        async def weights(request: Request):
            if not self._get_user(request): return JSONResponse({'error': 'Not authenticated'}, status_code=401)
            self.analyzer.update_weights(await request.json())
            return JSONResponse({'status': 'updated', 'weights': self.analyzer.weights})

        @self.app.get("/api/weights")
        async def get_weights(): return JSONResponse(self.analyzer.weights)

        @self.app.websocket("/ws")
        async def ws(websocket: WebSocket):
            await websocket.accept()
            self.websockets.add(websocket)
            try:
                while True: await asyncio.sleep(30); await websocket.send_json({'type': 'heartbeat'})
            except: self.websockets.discard(websocket)

    async def _broadcast(self, msg: Dict):
        for ws in list(self.websockets):
            try: await ws.send_json(msg)
            except: self.websockets.discard(ws)

    async def _run_scan(self):
        logger.info("Starting scan...")
        try:
            # Update coin list to include all user-watched coins
            all_watched = self.tracker.get_all_watched_coins()
            self.analyzer.coins = list(set(ALL_COINS + all_watched))
            logger.info(f"Scanning {len(self.analyzer.coins)} coins ({len(all_watched)} from watchlists)")
            self.scan_results = await self.analyzer.scan_all()
            self.last_scan = datetime.now()
            self.scan_count += 1
            self.tracker.log_batch(self.scan_results)
            logger.info(f"Scan #{self.scan_count}: {len(self.scan_results)} coins")
            await self._broadcast({'type': 'scan_complete', 'count': len(self.scan_results)})
        except Exception as e: logger.error(f"Scan error: {e}")

    async def scan_loop(self):
        while self.running: await self._run_scan(); await asyncio.sleep(self.scan_interval)

    async def aggregation_loop(self):
        while self.running:
            try:
                self.tracker.aggregate_daily_summaries()
                for d in range(2, 8): self.tracker.aggregate_daily_summaries((datetime.utcnow() - timedelta(days=d)).strftime('%Y-%m-%d'))
                if datetime.utcnow().day == 1: self.tracker.cleanup_old_snapshots(90)
            except Exception as e: logger.error(f"Agg error: {e}")
            await asyncio.sleep(3600)

    def _get_login_html(self) -> str:
        return '''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Login</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:linear-gradient(135deg,#0a0a0f,#1a1a2e);min-height:100vh;display:flex;align-items:center;justify-content:center;color:#e8e8ff}
.c{background:#12121a;border:1px solid #2d2d4a;border-radius:16px;padding:40px;text-align:center;max-width:400px;width:90%}.logo{width:60px;height:60px;background:linear-gradient(135deg,#8b5cf6,#00b4ff);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:24px;font-weight:bold;margin:0 auto 20px}
h1{font-size:24px;margin-bottom:10px}p{color:#8888aa;margin-bottom:30px}.btn{display:inline-flex;align-items:center;gap:12px;background:#fff;color:#333;padding:12px 24px;border-radius:8px;font-size:16px;font-weight:500;text-decoration:none}
.btn:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.3)}.f{margin-top:40px;text-align:left;padding:20px;background:#1a1a2e;border-radius:8px}.f h3{font-size:14px;margin-bottom:15px;color:#8b5cf6}.f ul{list-style:none}.f li{padding:8px 0;color:#8888aa;font-size:14px}.f li::before{content:"✓";color:#00d68f;margin-right:8px}</style></head>
<body><div class="c"><div class="logo">CA</div><h1>Crypto Analyzer</h1><p>Real-time market analysis with OBV signals</p>
<a href="/auth/login" class="btn"><svg width="20" height="20" viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>Sign in with Google</a>
<div class="f"><h3>Features</h3><ul><li>Real-time OBV analysis for 35+ coins</li><li>Custom watchlist management</li><li>Historical score tracking</li><li>Divergence detection</li></ul></div></div></body></html>'''

    def _get_dashboard_html(self) -> str:
        try:
            with open('templates/dashboard.html', 'r', encoding='utf-8') as f: return f.read()
        except: return "<html><body><h1>Dashboard not found</h1></body></html>"


app_instance = None
def get_app():
    global app_instance
    if app_instance is None: app_instance = AnalyzerApp()
    return app_instance

app = get_app().app

@app.on_event("startup")
async def startup():
    inst = get_app()
    inst.running = True
    asyncio.create_task(inst.scan_loop())
    asyncio.create_task(inst.aggregation_loop())
    logger.info("Started")

@app.on_event("shutdown")
async def shutdown(): get_app().running = False

if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)