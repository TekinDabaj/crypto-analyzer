#!/usr/bin/env python3
"""
Crypto Market Analyzer - Binance Version
v2.2 - Production-grade historical tracking with daily aggregation
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import ccxt
import numpy as np
import pandas as pd
import ta as ta_lib
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv('ANALYZER_DB_PATH', 'history.db')


class HistoricalTracker:
    """
    Production-grade historical tracking with:
    - Connection pooling via context manager
    - Pre-computed daily summaries
    - Efficient pagination
    - Background aggregation
    """
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Thread-safe connection context manager."""
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
        """Initialize database with optimized schema."""
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS coin_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    score INTEGER NOT NULL,
                    grade TEXT NOT NULL,
                    obv_trend TEXT,
                    obv_divergence TEXT,
                    funding_rate REAL,
                    rsi REAL,
                    volume_ratio REAL,
                    signals TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    scan_count INTEGER NOT NULL,
                    avg_score REAL NOT NULL,
                    avg_grade TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    price_change_pct REAL NOT NULL,
                    dominant_obv_trend TEXT,
                    bullish_divergence_count INTEGER DEFAULT 0,
                    bearish_divergence_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(date, symbol)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_date ON coin_snapshots(symbol, date DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_date ON coin_snapshots(date DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON coin_snapshots(timestamp DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_summaries_symbol_date ON daily_summaries(symbol, date DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_summaries_date ON daily_summaries(date DESC)")
            
        logger.info(f"Historical tracker initialized: {self.db_path}")
    
    def log_snapshot(self, coin_data: Dict):
        """Log a single coin analysis snapshot."""
        timestamp = coin_data['timestamp']
        snapshot_date = timestamp[:10]
        
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO coin_snapshots 
                (timestamp, date, symbol, price, score, grade, obv_trend, obv_divergence,
                 funding_rate, rsi, volume_ratio, signals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp,
                snapshot_date,
                coin_data['symbol'],
                coin_data['price'],
                coin_data['score']['total_score'],
                coin_data['score']['grade'],
                coin_data['indicators']['1h'].get('obv_trend'),
                coin_data['indicators']['1h'].get('obv_divergence'),
                coin_data['funding_rate'],
                coin_data['indicators']['1h'].get('rsi'),
                coin_data['indicators']['1h'].get('volume_ratio'),
                json.dumps(coin_data['signals'])
            ))
    
    def log_batch(self, coins: List[Dict]):
        """Log multiple coin snapshots efficiently."""
        if not coins:
            return
            
        timestamp = coins[0]['timestamp']
        snapshot_date = timestamp[:10]
        
        with self.get_connection() as conn:
            conn.executemany("""
                INSERT INTO coin_snapshots 
                (timestamp, date, symbol, price, score, grade, obv_trend, obv_divergence,
                 funding_rate, rsi, volume_ratio, signals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    coin['timestamp'],
                    snapshot_date,
                    coin['symbol'],
                    coin['price'],
                    coin['score']['total_score'],
                    coin['score']['grade'],
                    coin['indicators']['1h'].get('obv_trend'),
                    coin['indicators']['1h'].get('obv_divergence'),
                    coin['funding_rate'],
                    coin['indicators']['1h'].get('rsi'),
                    coin['indicators']['1h'].get('volume_ratio'),
                    json.dumps(coin['signals'])
                )
                for coin in coins
            ])
    
    def aggregate_daily_summaries(self, target_date: Optional[str] = None):
        """Aggregate snapshots into daily summaries for completed days."""
        if target_date is None:
            target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        with self.get_connection() as conn:
            existing = conn.execute(
                "SELECT COUNT(*) FROM daily_summaries WHERE date = ?",
                (target_date,)
            ).fetchone()[0]
            
            if existing > 0:
                logger.debug(f"Daily summaries for {target_date} already exist")
                return
            
            symbols = conn.execute(
                "SELECT DISTINCT symbol FROM coin_snapshots WHERE date = ?",
                (target_date,)
            ).fetchall()
            
            for (symbol,) in symbols:
                self._aggregate_coin_day(conn, symbol, target_date)
            
            logger.info(f"Aggregated {len(symbols)} coins for {target_date}")
    
    def _aggregate_coin_day(self, conn, symbol: str, target_date: str):
        """Aggregate a single coin's data for a specific day."""
        rows = conn.execute("""
            SELECT price, score, grade, obv_trend, obv_divergence, timestamp
            FROM coin_snapshots
            WHERE symbol = ? AND date = ?
            ORDER BY timestamp ASC
        """, (symbol, target_date)).fetchall()
        
        if not rows:
            return
        
        prices = [r['price'] for r in rows]
        scores = [r['score'] for r in rows]
        obv_trends = [r['obv_trend'] for r in rows if r['obv_trend']]
        
        avg_score = sum(scores) / len(scores)
        open_price = prices[0]
        close_price = prices[-1]
        high_price = max(prices)
        low_price = min(prices)
        price_change_pct = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0
        
        avg_grade = (
            'A+' if avg_score >= 90 else
            'A' if avg_score >= 80 else
            'B' if avg_score >= 70 else
            'C' if avg_score >= 60 else
            'D' if avg_score >= 50 else
            'F'
        )
        
        dominant_obv = 'neutral'
        if obv_trends:
            from collections import Counter
            trend_counts = Counter(obv_trends)
            dominant_obv = trend_counts.most_common(1)[0][0]
        
        bullish_div = sum(1 for r in rows if r['obv_divergence'] == 'bullish')
        bearish_div = sum(1 for r in rows if r['obv_divergence'] == 'bearish')
        
        conn.execute("""
            INSERT OR REPLACE INTO daily_summaries
            (date, symbol, scan_count, avg_score, avg_grade, open_price, close_price,
             high_price, low_price, price_change_pct, dominant_obv_trend,
             bullish_divergence_count, bearish_divergence_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            target_date,
            symbol,
            len(rows),
            round(avg_score, 1),
            avg_grade,
            open_price,
            close_price,
            high_price,
            low_price,
            round(price_change_pct, 2),
            dominant_obv,
            bullish_div,
            bearish_div,
            datetime.utcnow().isoformat()
        ))
    
    def get_coin_daily_history(
        self,
        symbol: str,
        limit: int = 30,
        before_date: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """Get paginated daily history for a coin."""
        with self.get_connection() as conn:
            if before_date:
                rows = conn.execute("""
                    SELECT date, avg_score, avg_grade, open_price, close_price,
                           price_change_pct, scan_count, dominant_obv_trend,
                           bullish_divergence_count, bearish_divergence_count
                    FROM daily_summaries
                    WHERE symbol = ? AND date < ?
                    ORDER BY date DESC
                    LIMIT ?
                """, (symbol, before_date, limit + 1)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT date, avg_score, avg_grade, open_price, close_price,
                           price_change_pct, scan_count, dominant_obv_trend,
                           bullish_divergence_count, bearish_divergence_count
                    FROM daily_summaries
                    WHERE symbol = ?
                    ORDER BY date DESC
                    LIMIT ?
                """, (symbol, limit + 1)).fetchall()
            
            has_more = len(rows) > limit
            rows = rows[:limit]
            
            results = [
                {
                    'date': row['date'],
                    'avg_score': row['avg_score'],
                    'avg_grade': row['avg_grade'],
                    'open_price': row['open_price'],
                    'close_price': row['close_price'],
                    'price_change_pct': row['price_change_pct'],
                    'scan_count': row['scan_count'],
                    'dominant_obv_trend': row['dominant_obv_trend'],
                    'bullish_divergence_count': row['bullish_divergence_count'],
                    'bearish_divergence_count': row['bearish_divergence_count']
                }
                for row in rows
            ]
            
            next_cursor = rows[-1]['date'] if has_more and rows else None
            
            return results, next_cursor
    
    def get_today_stats(self, symbol: str) -> Optional[Dict]:
        """Get today's running stats (not yet aggregated)."""
        today = datetime.utcnow().strftime('%Y-%m-%d')
        
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT price, score, grade
                FROM coin_snapshots
                WHERE symbol = ? AND date = ?
                ORDER BY timestamp ASC
            """, (symbol, today)).fetchall()
            
            if not rows:
                return None
            
            prices = [r['price'] for r in rows]
            scores = [r['score'] for r in rows]
            
            avg_score = sum(scores) / len(scores)
            avg_grade = (
                'A+' if avg_score >= 90 else
                'A' if avg_score >= 80 else
                'B' if avg_score >= 70 else
                'C' if avg_score >= 60 else
                'D' if avg_score >= 50 else
                'F'
            )
            
            return {
                'date': today,
                'avg_score': round(avg_score, 1),
                'avg_grade': avg_grade,
                'open_price': prices[0],
                'current_price': prices[-1],
                'price_change_pct': round((prices[-1] - prices[0]) / prices[0] * 100, 2) if prices[0] > 0 else 0,
                'scan_count': len(rows),
                'is_today': True
            }
    
    def get_grade_performance_summary(self, days: int = 30) -> Dict:
        """Get performance summary grouped by grade."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT 
                    avg_grade as grade,
                    COUNT(*) as count,
                    AVG(price_change_pct) as avg_change,
                    SUM(CASE WHEN price_change_pct > 0 THEN 1 ELSE 0 END) as positive_count
                FROM daily_summaries
                WHERE date >= ?
                GROUP BY avg_grade
            """, (cutoff,)).fetchall()
            
            return {
                row['grade']: {
                    'count': row['count'],
                    'avg_change': round(row['avg_change'], 2) if row['avg_change'] else 0,
                    'win_rate': round(row['positive_count'] / row['count'] * 100, 1) if row['count'] > 0 else 0
                }
                for row in rows
            }
    
    def cleanup_old_snapshots(self, keep_days: int = 90):
        """Remove old raw snapshots to save space (keeps summaries)."""
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime('%Y-%m-%d')
        
        with self.get_connection() as conn:
            result = conn.execute(
                "DELETE FROM coin_snapshots WHERE date < ?",
                (cutoff,)
            )
            if result.rowcount > 0:
                logger.info(f"Cleaned up {result.rowcount} old snapshots")
                conn.execute("VACUUM")


class MarketAnalyzer:
    def __init__(self):
        self.exchange = self._init_exchange()
        self.spot_exchange = self._init_spot_exchange()
        
        self.coins = [
            'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
            'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
            'ATOM/USDT', 'NEAR/USDT', 'APT/USDT', 'ARB/USDT', 'OP/USDT',
            'SUI/USDT', 'INJ/USDT', 'FET/USDT', 'TIA/USDT', 'SEI/USDT',
            'DOGE/USDT', 'PEPE/USDT', 'WIF/USDT', 'SHIB/USDT', 'LTC/USDT',
            'BCH/USDT', 'ETC/USDT', 'FIL/USDT', 'IMX/USDT', 'RENDER/USDT',
            'MAGIC/USDT', 'LIT/USDT', 'ZEN/USDT', 'ZEC/USDT', 'PUMP/USDT'
        ]
        
        self.weights = {
            'volume': 20,
            'obv': 35,
            'funding': 20,
            'rsi': 25
        }
        self._normalize_weights()
        
        self.obv_lookback = 14
        self.rsi_period = 14
        
        self.cache = {}
        self.cache_duration = 60
        
        logger.info(f"Market Analyzer initialized with {len(self.coins)} coins")
    
    def _normalize_weights(self):
        total = sum(self.weights.values())
        if total != 100:
            factor = 100 / total
            self.weights = {k: round(v * factor, 1) for k, v in self.weights.items()}
    
    def update_weights(self, new_weights: Dict[str, float]):
        for key in ['volume', 'obv', 'funding', 'rsi']:
            if key in new_weights:
                self.weights[key] = new_weights[key]
        self._normalize_weights()
        logger.info(f"Weights updated: {self.weights}")
    
    def _init_exchange(self) -> ccxt.Exchange:
        api_key = os.getenv('BINANCE_API_KEY', '')
        secret_key = os.getenv('BINANCE_SECRET_KEY', '')
        
        exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': secret_key,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        })
        
        try:
            exchange.load_markets()
            logger.info(f"Connected to Binance - {len(exchange.markets)} markets")
        except Exception as e:
            logger.error(f"Failed to connect to Binance: {e}")
        
        return exchange
    
    def _init_spot_exchange(self) -> ccxt.Exchange:
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        try:
            exchange.load_markets()
            logger.info("Connected to Binance Spot")
        except Exception as e:
            logger.error(f"Failed to connect to Binance Spot: {e}")
        return exchange
    
    async def get_ohlcv(self, symbol: str, timeframe: str = '1h', limit: int = 200) -> pd.DataFrame:
        cache_key = f"{symbol}_{timeframe}_{limit}"
        now = time.time()
        
        if cache_key in self.cache:
            if now - self.cache[cache_key]['time'] < self.cache_duration:
                return self.cache[cache_key]['data'].copy()
        
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            
            if not ohlcv or len(ohlcv) < 50:
                return pd.DataFrame()
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            self.cache[cache_key] = {'data': df.copy(), 'time': now}
            return df
            
        except Exception as e:
            logger.debug(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()
    
    async def get_funding_rate(self, symbol: str) -> float:
        try:
            funding = self.exchange.fetch_funding_rate(symbol)
            return funding.get('fundingRate', 0) or 0
        except:
            return 0
    
    async def get_spot_volume(self, symbol: str) -> Dict:
        try:
            ohlcv = self.spot_exchange.fetch_ohlcv(symbol, '1h', limit=48)
            if ohlcv and len(ohlcv) >= 48:
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['vol_usdt'] = df['volume'] * df['close']
                
                vol_24h = float(df['vol_usdt'].iloc[-24:].sum())
                vol_prev_24h = float(df['vol_usdt'].iloc[-48:-24].sum())
                vol_change = ((vol_24h - vol_prev_24h) / vol_prev_24h * 100) if vol_prev_24h > 0 else 0
                
                return {'volume': vol_24h, 'change': round(vol_change, 1)}
            return {'volume': 0, 'change': 0}
        except:
            return {'volume': 0, 'change': 0}
    
    def calculate_indicators(self, df: pd.DataFrame) -> Dict:
        if df.empty or len(df) < 50:
            return {}
        
        try:
            close = df['close']
            high = df['high']
            low = df['low']
            volume = df['volume']
            
            current_price = float(close.iloc[-1])
            
            # ATR using ta library
            atr_series = ta_lib.volatility.average_true_range(high, low, close, window=14)
            atr = float(atr_series.iloc[-1]) if atr_series is not None and not pd.isna(atr_series.iloc[-1]) else 0
            atr_percent = (atr / current_price * 100) if current_price > 0 else 2.0
            
            divergence_threshold = max(1.5, min(atr_percent * 1.5, 5.0))
            
            if len(volume) >= 48:
                volume_24h = float(volume.iloc[-25:-1].sum())
                volume_prev_24h = float(volume.iloc[-49:-25].sum())
                volume_ratio = volume_24h / volume_prev_24h if volume_prev_24h > 0 else 1.0
            else:
                volume_ma = float(volume.iloc[-20:-1].mean())
                last_volume = float(volume.iloc[-2])
                volume_ratio = last_volume / volume_ma if volume_ma > 0 else 1.0
            
            # OBV using ta library
            obv_series = ta_lib.volume.on_balance_volume(close, volume)
            if obv_series is not None and len(obv_series) >= self.obv_lookback:
                obv_current = float(obv_series.iloc[-1])
                obv_prev = float(obv_series.iloc[-self.obv_lookback])
                obv_change = ((obv_current - obv_prev) / abs(obv_prev) * 100) if obv_prev != 0 else 0
                
                obv_recent = obv_series.tail(14).values
                obv_slope = np.polyfit(range(len(obv_recent)), obv_recent, 1)[0]
                obv_slope_norm = obv_slope / abs(obv_current) * 1000 if obv_current != 0 else 0
                
                if obv_slope_norm > 0.5:
                    obv_trend = 'bullish'
                    obv_strength = 'strong' if obv_slope_norm > 1.5 else 'moderate'
                elif obv_slope_norm < -0.5:
                    obv_trend = 'bearish'
                    obv_strength = 'strong' if obv_slope_norm < -1.5 else 'moderate'
                else:
                    obv_trend = 'neutral'
                    obv_strength = 'weak'
                
                price_change_14 = ((current_price - float(close.iloc[-14])) / float(close.iloc[-14]) * 100)
                obv_divergence = 'none'
                
                if price_change_14 < -divergence_threshold and obv_change > divergence_threshold:
                    obv_divergence = 'bullish'
                elif price_change_14 > divergence_threshold and obv_change < -divergence_threshold:
                    obv_divergence = 'bearish'
            else:
                obv_current = obv_change = 0
                obv_trend = 'neutral'
                obv_strength = 'weak'
                obv_divergence = 'none'
                divergence_threshold = 2.0
            
            # RSI using ta library
            rsi_series = ta_lib.momentum.rsi(close, window=self.rsi_period)
            current_rsi = float(rsi_series.iloc[-1]) if rsi_series is not None and not pd.isna(rsi_series.iloc[-1]) else 50
            
            # SMAs using ta library
            sma_20_series = ta_lib.trend.sma_indicator(close, window=20)
            sma_50_series = ta_lib.trend.sma_indicator(close, window=50) if len(close) >= 50 else None
            sma_200_series = ta_lib.trend.sma_indicator(close, window=200) if len(close) >= 200 else None
            
            sma_20_val = float(sma_20_series.iloc[-1]) if sma_20_series is not None and not pd.isna(sma_20_series.iloc[-1]) else current_price
            sma_50_val = float(sma_50_series.iloc[-1]) if sma_50_series is not None and not pd.isna(sma_50_series.iloc[-1]) else sma_20_val
            sma_200_val = float(sma_200_series.iloc[-1]) if sma_200_series is not None and not pd.isna(sma_200_series.iloc[-1]) else sma_50_val
            
            if current_price > sma_20_val > sma_50_val:
                trend = 'uptrend'
            elif current_price < sma_20_val < sma_50_val:
                trend = 'downtrend'
            else:
                trend = 'sideways'
            
            price_change_24h = ((current_price - float(close.iloc[-24])) / float(close.iloc[-24]) * 100) if len(close) >= 24 else 0
            
            # MACD using ta library
            macd_line = ta_lib.trend.macd(close)
            macd_signal = ta_lib.trend.macd_signal(close)
            macd_bullish = False
            if macd_line is not None and macd_signal is not None:
                if not pd.isna(macd_line.iloc[-1]) and not pd.isna(macd_signal.iloc[-1]):
                    macd_bullish = float(macd_line.iloc[-1]) > float(macd_signal.iloc[-1])
            
            return {
                'price': round(current_price, 6),
                'price_change_24h': round(price_change_24h, 2),
                'volume_ratio': round(volume_ratio, 2),
                'obv': round(obv_current, 2),
                'obv_change': round(obv_change, 2),
                'obv_trend': obv_trend,
                'obv_strength': obv_strength,
                'obv_divergence': obv_divergence,
                'divergence_threshold': round(divergence_threshold, 2),
                'rsi': round(current_rsi, 2),
                'sma_20': round(sma_20_val, 6),
                'sma_50': round(sma_50_val, 6),
                'sma_200': round(sma_200_val, 6),
                'trend': trend,
                'atr_percent': round(atr_percent, 2),
                'macd_bullish': macd_bullish
            }
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}
    
    def score_coin(self, indicators: Dict, funding_rate: float) -> Dict:
        scores = {}
        details = []
        
        volume_ratio = indicators.get('volume_ratio', 1.0)
        if volume_ratio >= 1.5:
            scores['volume'] = 100
            details.append(('volume', 'high', self.weights['volume']))
        elif volume_ratio >= 1.1:
            scores['volume'] = 80
            details.append(('volume', 'good', round(self.weights['volume'] * 0.8, 1)))
        elif volume_ratio >= 0.8:
            scores['volume'] = 60
            details.append(('volume', 'normal', round(self.weights['volume'] * 0.6, 1)))
        else:
            scores['volume'] = 40
            details.append(('volume', 'low', round(self.weights['volume'] * 0.4, 1)))
        
        obv_trend = indicators.get('obv_trend', 'neutral')
        obv_strength = indicators.get('obv_strength', 'weak')
        obv_divergence = indicators.get('obv_divergence', 'none')
        
        if obv_trend == 'bullish' and obv_strength == 'strong':
            scores['obv'] = 100
            details.append(('obv', 'strong_buying', self.weights['obv']))
        elif obv_trend == 'bullish':
            scores['obv'] = 80
            details.append(('obv', 'buying', round(self.weights['obv'] * 0.8, 1)))
        elif obv_trend == 'bearish' and obv_strength == 'strong':
            scores['obv'] = 20
            details.append(('obv', 'strong_selling', round(self.weights['obv'] * 0.2, 1)))
        elif obv_trend == 'bearish':
            scores['obv'] = 40
            details.append(('obv', 'selling', round(self.weights['obv'] * 0.4, 1)))
        else:
            scores['obv'] = 60
            details.append(('obv', 'neutral', round(self.weights['obv'] * 0.6, 1)))
        
        if obv_divergence == 'bullish':
            scores['obv'] = min(100, scores['obv'] + 15)
        elif obv_divergence == 'bearish':
            scores['obv'] = max(0, scores['obv'] - 15)
        
        funding_pct = funding_rate * 100
        if -0.005 <= funding_pct <= 0.005:
            scores['funding'] = 100
            details.append(('funding', 'neutral', self.weights['funding']))
        elif -0.01 <= funding_pct <= 0.01:
            scores['funding'] = 80
            details.append(('funding', 'healthy', round(self.weights['funding'] * 0.8, 1)))
        elif -0.02 <= funding_pct <= 0.02:
            scores['funding'] = 60
            details.append(('funding', 'moderate', round(self.weights['funding'] * 0.6, 1)))
        else:
            scores['funding'] = 20
            details.append(('funding', 'extreme', round(self.weights['funding'] * 0.2, 1)))
        
        rsi = indicators.get('rsi', 50)
        if 40 <= rsi <= 60:
            scores['rsi'] = 100
            details.append(('rsi', 'neutral', self.weights['rsi']))
        elif 30 <= rsi <= 70:
            scores['rsi'] = 80
            details.append(('rsi', 'healthy', round(self.weights['rsi'] * 0.8, 1)))
        elif 20 <= rsi <= 80:
            scores['rsi'] = 40
            details.append(('rsi', 'extended', round(self.weights['rsi'] * 0.4, 1)))
        else:
            scores['rsi'] = 20
            details.append(('rsi', 'extreme', round(self.weights['rsi'] * 0.2, 1)))
        
        total_score = sum(scores[k] * (self.weights[k] / 100) for k in scores)
        
        grade = (
            'A+' if total_score >= 90 else
            'A' if total_score >= 80 else
            'B' if total_score >= 70 else
            'C' if total_score >= 60 else
            'D' if total_score >= 50 else
            'F'
        )
        
        return {
            'total_score': round(total_score),
            'percentage': round(total_score, 1),
            'breakdown': details,
            'component_scores': scores,
            'grade': grade,
            'weights_used': self.weights.copy()
        }
    
    async def analyze_coin(self, symbol: str) -> Dict:
        try:
            df_1h = await self.get_ohlcv(symbol, '1h', 200)
            df_4h = await self.get_ohlcv(symbol, '4h', 100)
            
            if df_1h.empty:
                return None
            
            indicators_1h = self.calculate_indicators(df_1h)
            indicators_4h = self.calculate_indicators(df_4h) if not df_4h.empty else {}
            
            if not indicators_1h:
                return None
            
            funding_rate = await self.get_funding_rate(symbol)
            spot_data = await self.get_spot_volume(symbol)
            
            price = indicators_1h.get('price', 1)
            vol_24h = float(df_1h['volume'].iloc[-25:-1].sum()) * price
            vol_prev_24h = float(df_1h['volume'].iloc[-49:-25].sum()) * price if len(df_1h) >= 49 else vol_24h
            futures_vol_change = ((vol_24h - vol_prev_24h) / vol_prev_24h * 100) if vol_prev_24h > 0 else 0
            
            score_data = self.score_coin(indicators_1h, funding_rate)
            
            display_name = symbol.replace('/USDT', '')
            
            return {
                'symbol': symbol,
                'display_name': display_name,
                'timestamp': datetime.utcnow().isoformat(),
                'price': indicators_1h.get('price', 0),
                'price_change_24h': indicators_1h.get('price_change_24h', 0),
                'indicators': {'1h': indicators_1h, '4h': indicators_4h},
                'funding_rate': round(funding_rate * 100, 4),
                'spot_volume': round(spot_data['volume'], 0),
                'spot_volume_change': round(spot_data['change'], 1),
                'futures_volume': round(vol_24h, 0),
                'futures_volume_change': round(futures_vol_change, 1),
                'score': score_data,
                'signals': self._generate_signals(indicators_1h, indicators_4h, funding_rate)
            }
        except Exception as e:
            logger.debug(f"Error analyzing {symbol}: {e}")
            return None
    
    def _generate_signals(self, ind_1h: Dict, ind_4h: Dict, funding: float) -> List[str]:
        signals = []
        
        vol_ratio = ind_1h.get('volume_ratio', 1.0)
        if vol_ratio >= 1.5:
            signals.append("[VOL] High 24h activity")
        elif vol_ratio <= 0.6:
            signals.append("[VOL] Low 24h activity")
        
        obv_trend = ind_1h.get('obv_trend', 'neutral')
        obv_strength = ind_1h.get('obv_strength', 'weak')
        obv_divergence = ind_1h.get('obv_divergence', 'none')
        
        if obv_trend == 'bullish':
            if obv_strength == 'strong':
                signals.append("[OBV] Strong accumulation")
            else:
                signals.append("[OBV] Accumulation")
        elif obv_trend == 'bearish':
            if obv_strength == 'strong':
                signals.append("[OBV] Strong distribution")
            else:
                signals.append("[OBV] Distribution")
        
        if obv_divergence == 'bullish':
            signals.append("[OBV] Bullish divergence!")
        elif obv_divergence == 'bearish':
            signals.append("[OBV] Bearish divergence!")
        
        rsi = ind_1h.get('rsi', 50)
        if rsi < 30:
            signals.append("[RSI] Oversold")
        elif rsi > 70:
            signals.append("[RSI] Overbought")
        
        funding_pct = funding * 100
        if funding_pct > 0.03:
            signals.append("[FUND] Longs paying")
        elif funding_pct < -0.03:
            signals.append("[FUND] Shorts paying")
        
        trend_1h = ind_1h.get('trend', 'sideways')
        trend_4h = ind_4h.get('trend', 'sideways') if ind_4h else 'sideways'
        
        if trend_1h == 'uptrend' and trend_4h == 'uptrend':
            signals.append("[TREND] Bullish alignment")
        elif trend_1h == 'downtrend' and trend_4h == 'downtrend':
            signals.append("[TREND] Bearish alignment")
        
        if ind_1h.get('macd_bullish'):
            signals.append("[MACD] Bullish")
        
        return signals if signals else ["No signals"]
    
    async def scan_all(self) -> List[Dict]:
        results = []
        for symbol in self.coins:
            try:
                result = await self.analyze_coin(symbol)
                if result:
                    results.append(result)
                await asyncio.sleep(0.15)
            except Exception as e:
                logger.debug(f"Error scanning {symbol}: {e}")
        results.sort(key=lambda x: x['score']['total_score'], reverse=True)
        return results


class AnalyzerApp:
    def __init__(self):
        self.analyzer = MarketAnalyzer()
        self.tracker = HistoricalTracker()
        self.app = FastAPI(title="Crypto Market Analyzer", version="2.2.0")
        self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
        
        self.running = False
        self.scan_results = []
        self.last_scan = None
        self.scan_count = 0
        self.scan_interval = 300
        self.websockets: Set[WebSocket] = set()
        self.startup_time = datetime.now()
        
        self._setup_routes()
        logger.info("Analyzer App initialized")
    
    def _setup_routes(self):
        @self.app.get("/")
        async def dashboard():
            return HTMLResponse(content=self._get_dashboard_html())
        
        @self.app.get("/api/scan")
        async def get_scan():
            return JSONResponse(content={
                'results': self.scan_results,
                'last_scan': self.last_scan.isoformat() if self.last_scan else None,
                'scan_count': self.scan_count,
                'weights': self.analyzer.weights
            })
        
        @self.app.get("/api/coin/{symbol}")
        async def get_coin(symbol: str):
            symbol = symbol.upper()
            if '/' not in symbol:
                symbol = symbol + '/USDT'
            result = await self.analyzer.analyze_coin(symbol)
            return JSONResponse(content=result or {'error': 'Not found'})
        
        @self.app.get("/api/coin/{symbol}/history")
        async def get_coin_history(
            symbol: str,
            limit: int = Query(default=30, ge=1, le=100),
            before: Optional[str] = Query(default=None, description="Cursor for pagination (date string)")
        ):
            """Get paginated daily history for a coin."""
            symbol = symbol.upper()
            if '/' not in symbol:
                symbol = symbol + '/USDT'
            
            today_stats = self.tracker.get_today_stats(symbol)
            history, next_cursor = self.tracker.get_coin_daily_history(
                symbol, limit=limit, before_date=before
            )
            
            return JSONResponse(content={
                'symbol': symbol,
                'today': today_stats,
                'history': history,
                'next_cursor': next_cursor,
                'has_more': next_cursor is not None
            })
        
        @self.app.post("/api/scan/trigger")
        async def trigger():
            asyncio.create_task(self._run_scan())
            return JSONResponse(content={'status': 'triggered'})
        
        @self.app.post("/api/weights")
        async def update_weights(weights: Dict[str, float]):
            self.analyzer.update_weights(weights)
            return JSONResponse(content={'status': 'updated', 'weights': self.analyzer.weights})
        
        @self.app.get("/api/weights")
        async def get_weights():
            return JSONResponse(content=self.analyzer.weights)
        
        @self.app.get("/api/history/performance")
        async def get_performance(days: int = Query(default=30, ge=1, le=365)):
            return JSONResponse(content={
                'grade_performance': self.tracker.get_grade_performance_summary(days),
                'days_analyzed': days
            })
        
        @self.app.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.websockets.add(websocket)
            try:
                while True:
                    await asyncio.sleep(30)
                    await websocket.send_json({'type': 'heartbeat'})
            except:
                self.websockets.discard(websocket)
    
    async def _broadcast(self, msg: Dict):
        for ws in list(self.websockets):
            try:
                await ws.send_json(msg)
            except:
                self.websockets.discard(ws)
    
    async def _run_scan(self):
        logger.info("Starting scan...")
        try:
            self.scan_results = await self.analyzer.scan_all()
            self.last_scan = datetime.now()
            self.scan_count += 1
            
            self.tracker.log_batch(self.scan_results)
            
            logger.info(f"Scan #{self.scan_count}: {len(self.scan_results)} coins")
            await self._broadcast({'type': 'scan_complete', 'count': len(self.scan_results)})
        except Exception as e:
            logger.error(f"Scan error: {e}")
    
    async def scan_loop(self):
        while self.running:
            await self._run_scan()
            await asyncio.sleep(self.scan_interval)
    
    async def aggregation_loop(self):
        """Background task to aggregate daily summaries."""
        while self.running:
            try:
                now = datetime.utcnow()
                self.tracker.aggregate_daily_summaries()
                
                for days_ago in range(2, 8):
                    target = (now - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                    self.tracker.aggregate_daily_summaries(target)
                
                if now.day == 1 and now.hour == 0:
                    self.tracker.cleanup_old_snapshots(keep_days=90)
                    
            except Exception as e:
                logger.error(f"Aggregation error: {e}")
            
            await asyncio.sleep(3600)
    
    def _get_dashboard_html(self) -> str:
        try:
            with open('templates/dashboard.html', 'r', encoding='utf-8') as f:
                return f.read()
        except:
            return "<html><body><h1>Dashboard not found</h1></body></html>"


app_instance = None

def get_app():
    global app_instance
    if app_instance is None:
        app_instance = AnalyzerApp()
    return app_instance

app = get_app().app

@app.on_event("startup")
async def startup():
    instance = get_app()
    instance.running = True
    asyncio.create_task(instance.scan_loop())
    asyncio.create_task(instance.aggregation_loop())
    logger.info("Started")

@app.on_event("shutdown")
async def shutdown():
    get_app().running = False

if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)