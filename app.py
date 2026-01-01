#!/usr/bin/env python3
"""
Crypto Market Analyzer - Binance Version
Scans the market for coins with good spot volume, healthy OBV, reasonable funding rates, and RSI levels.
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Set

import ccxt
import numpy as np
import pandas as pd
import pandas_ta as ta
import psutil
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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


class MarketAnalyzer:
    def __init__(self):
        self.exchange = self._init_exchange()
        
        self.coins = [
            'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT',
            'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
            'ATOM/USDT', 'NEAR/USDT', 'APT/USDT', 'ARB/USDT', 'OP/USDT',
            'SUI/USDT', 'INJ/USDT', 'FET/USDT', 'TIA/USDT', 'SEI/USDT',
            'DOGE/USDT', 'PEPE/USDT', 'WIF/USDT', 'SHIB/USDT', 'LTC/USDT',
            'BCH/USDT', 'ETC/USDT', 'FIL/USDT', 'IMX/USDT', 'RENDER/USDT'
        ]
        
        self.volume_ma_period = 20
        self.obv_lookback = 14
        self.rsi_period = 14
        
        self.cache = {}
        self.cache_duration = 60
        
        logger.info(f"Market Analyzer initialized with {len(self.coins)} coins")
    
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
    
    def calculate_indicators(self, df: pd.DataFrame) -> Dict:
        if df.empty or len(df) < 50:
            return {}
        
        try:
            close = df['close']
            high = df['high']
            low = df['low']
            volume = df['volume']
            
            current_price = float(close.iloc[-1])
            
            volume_ma = float(volume.tail(self.volume_ma_period).mean())
            current_volume = float(volume.iloc[-1])
            volume_ratio = current_volume / volume_ma if volume_ma > 0 else 0
            avg_volume_24h = float(volume.tail(24).mean()) if len(volume) >= 24 else volume_ma
            
            obv_series = ta.obv(close, volume)
            if obv_series is not None and len(obv_series) >= self.obv_lookback:
                obv_current = float(obv_series.iloc[-1])
                obv_prev = float(obv_series.iloc[-self.obv_lookback])
                obv_change = ((obv_current - obv_prev) / abs(obv_prev) * 100) if obv_prev != 0 else 0
                obv_recent = obv_series.tail(14).values
                obv_slope = np.polyfit(range(len(obv_recent)), obv_recent, 1)[0]
                obv_trend = 'bullish' if obv_slope > 0 else 'bearish' if obv_slope < 0 else 'neutral'
            else:
                obv_current = 0
                obv_change = 0
                obv_trend = 'neutral'
            
            rsi_series = ta.rsi(close, length=self.rsi_period)
            current_rsi = float(rsi_series.iloc[-1]) if rsi_series is not None and not pd.isna(rsi_series.iloc[-1]) else 50
            
            sma_20_series = ta.sma(close, length=20)
            sma_50_series = ta.sma(close, length=50)
            sma_200_series = ta.sma(close, length=200)
            
            sma_20 = float(sma_20_series.iloc[-1]) if sma_20_series is not None else current_price
            sma_50 = float(sma_50_series.iloc[-1]) if sma_50_series is not None and len(close) >= 50 else sma_20
            sma_200 = float(sma_200_series.iloc[-1]) if sma_200_series is not None and len(close) >= 200 else sma_50
            
            if current_price > sma_20 > sma_50:
                trend = 'uptrend'
            elif current_price < sma_20 < sma_50:
                trend = 'downtrend'
            else:
                trend = 'sideways'
            
            price_change_24h = ((current_price - float(close.iloc[-24])) / float(close.iloc[-24]) * 100) if len(close) >= 24 else 0
            price_change_1h = ((current_price - float(close.iloc[-2])) / float(close.iloc[-2]) * 100) if len(close) >= 2 else 0
            
            atr_series = ta.atr(high, low, close, length=14)
            atr = float(atr_series.iloc[-1]) if atr_series is not None else 0
            atr_percent = (atr / current_price * 100) if current_price > 0 else 0
            
            macd_df = ta.macd(close)
            if macd_df is not None and len(macd_df.columns) >= 2:
                macd_line = float(macd_df.iloc[-1, 0])
                macd_signal = float(macd_df.iloc[-1, 2])
                macd_bullish = macd_line > macd_signal
            else:
                macd_bullish = False
            
            return {
                'price': round(current_price, 6),
                'price_change_1h': round(price_change_1h, 2),
                'price_change_24h': round(price_change_24h, 2),
                'volume_current': round(current_volume, 2),
                'volume_avg': round(volume_ma, 2),
                'volume_ratio': round(volume_ratio, 2),
                'volume_24h_avg': round(avg_volume_24h, 2),
                'obv': round(obv_current, 2),
                'obv_change': round(obv_change, 2),
                'obv_trend': obv_trend,
                'rsi': round(current_rsi, 2),
                'sma_20': round(sma_20, 6),
                'sma_50': round(sma_50, 6),
                'sma_200': round(sma_200, 6),
                'trend': trend,
                'atr_percent': round(atr_percent, 2),
                'macd_bullish': macd_bullish
            }
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}
    
    def score_coin(self, indicators: Dict, funding_rate: float) -> Dict:
        score = 0
        max_score = 100
        details = []
        
        volume_ratio = indicators.get('volume_ratio', 0)
        if volume_ratio >= 2.0:
            score += 25
            details.append(('volume', 'excellent', 25))
        elif volume_ratio >= 1.5:
            score += 20
            details.append(('volume', 'good', 20))
        elif volume_ratio >= 1.0:
            score += 15
            details.append(('volume', 'average', 15))
        elif volume_ratio >= 0.5:
            score += 5
            details.append(('volume', 'low', 5))
        else:
            details.append(('volume', 'very_low', 0))
        
        obv_change = indicators.get('obv_change', 0)
        obv_trend = indicators.get('obv_trend', 'neutral')
        
        if obv_trend == 'bullish' and obv_change > 5:
            score += 25
            details.append(('obv', 'strong_bullish', 25))
        elif obv_trend == 'bullish':
            score += 20
            details.append(('obv', 'bullish', 20))
        elif obv_trend == 'bearish' and obv_change < -5:
            score += 5
            details.append(('obv', 'strong_bearish', 5))
        elif obv_trend == 'bearish':
            score += 10
            details.append(('obv', 'bearish', 10))
        else:
            score += 15
            details.append(('obv', 'neutral', 15))
        
        funding_pct = funding_rate * 100
        if -0.005 <= funding_pct <= 0.005:
            score += 25
            details.append(('funding', 'neutral', 25))
        elif -0.01 <= funding_pct <= 0.01:
            score += 20
            details.append(('funding', 'healthy', 20))
        elif -0.02 <= funding_pct <= 0.02:
            score += 15
            details.append(('funding', 'moderate', 15))
        elif abs(funding_pct) <= 0.05:
            score += 10
            details.append(('funding', 'elevated', 10))
        else:
            score += 5
            details.append(('funding', 'extreme', 5))
        
        rsi = indicators.get('rsi', 50)
        if 40 <= rsi <= 60:
            score += 25
            details.append(('rsi', 'neutral', 25))
        elif 30 <= rsi <= 70:
            score += 20
            details.append(('rsi', 'healthy', 20))
        elif 20 <= rsi <= 80:
            score += 10
            details.append(('rsi', 'extended', 10))
        else:
            score += 5
            details.append(('rsi', 'extreme', 5))
        
        return {
            'total_score': score,
            'max_score': max_score,
            'percentage': round(score / max_score * 100, 1),
            'breakdown': details,
            'grade': self._score_to_grade(score)
        }
    
    def _score_to_grade(self, score: int) -> str:
        if score >= 90:
            return 'A+'
        elif score >= 80:
            return 'A'
        elif score >= 70:
            return 'B'
        elif score >= 60:
            return 'C'
        elif score >= 50:
            return 'D'
        else:
            return 'F'
    
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
            score_data = self.score_coin(indicators_1h, funding_rate)
            
            display_name = symbol.replace('/USDT', '').replace('USDT', '')
            
            return {
                'symbol': symbol,
                'display_name': display_name,
                'timestamp': datetime.now().isoformat(),
                'price': indicators_1h.get('price', 0),
                'price_change_1h': indicators_1h.get('price_change_1h', 0),
                'price_change_24h': indicators_1h.get('price_change_24h', 0),
                'indicators': {
                    '1h': indicators_1h,
                    '4h': indicators_4h
                },
                'funding_rate': round(funding_rate * 100, 4),
                'funding_rate_daily': round(funding_rate * 100 * 3, 4),
                'score': score_data,
                'signals': self._generate_signals(indicators_1h, indicators_4h, funding_rate)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return None
    
    def _generate_signals(self, ind_1h: Dict, ind_4h: Dict, funding: float) -> List[str]:
        signals = []
        
        vol_ratio = ind_1h.get('volume_ratio', 0)
        if vol_ratio >= 2.0:
            signals.append("🔥 High volume spike")
        elif vol_ratio < 0.5:
            signals.append("⚠️ Volume dried up")
        
        obv_trend = ind_1h.get('obv_trend', 'neutral')
        obv_change = ind_1h.get('obv_change', 0)
        if obv_trend == 'bullish' and obv_change > 10:
            signals.append("📈 Strong OBV accumulation")
        elif obv_trend == 'bearish' and obv_change < -10:
            signals.append("📉 OBV showing distribution")
        
        rsi = ind_1h.get('rsi', 50)
        if rsi < 30:
            signals.append("🟢 RSI oversold")
        elif rsi > 70:
            signals.append("🔴 RSI overbought")
        
        funding_pct = abs(funding * 100)
        if funding_pct > 0.05:
            if funding > 0:
                signals.append("💰 High positive funding (longs paying)")
            else:
                signals.append("💰 High negative funding (shorts paying)")
        
        trend_1h = ind_1h.get('trend', 'sideways')
        trend_4h = ind_4h.get('trend', 'sideways') if ind_4h else 'sideways'
        
        if trend_1h == 'uptrend' and trend_4h == 'uptrend':
            signals.append("✅ Aligned uptrend (1H + 4H)")
        elif trend_1h == 'downtrend' and trend_4h == 'downtrend':
            signals.append("🚫 Aligned downtrend (1H + 4H)")
        
        if ind_1h.get('macd_bullish'):
            signals.append("📊 MACD bullish crossover")
        
        return signals if signals else ["➖ No significant signals"]
    
    async def scan_all(self) -> List[Dict]:
        results = []
        
        for symbol in self.coins:
            try:
                result = await self.analyze_coin(symbol)
                if result:
                    results.append(result)
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")
                continue
        
        results.sort(key=lambda x: x['score']['total_score'], reverse=True)
        return results


class AnalyzerApp:
    def __init__(self):
        self.analyzer = MarketAnalyzer()
        self.app = FastAPI(title="Crypto Market Analyzer", version="1.0.0")
        
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
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
        async def get_scan_results():
            return JSONResponse(content={
                'results': self.scan_results,
                'last_scan': self.last_scan.isoformat() if self.last_scan else None,
                'scan_count': self.scan_count,
                'coins_analyzed': len(self.scan_results)
            })
        
        @self.app.get("/api/coin/{symbol}")
        async def get_coin_detail(symbol: str):
            symbol = symbol.upper()
            if '/' not in symbol:
                symbol = symbol.replace('USDT', '') + '/USDT'
            result = await self.analyzer.analyze_coin(symbol)
            if result:
                return JSONResponse(content=result)
            return JSONResponse(content={'error': 'Coin not found'}, status_code=404)
        
        @self.app.get("/api/top")
        async def get_top_coins():
            top = [r for r in self.scan_results if r['score']['percentage'] >= 70]
            return JSONResponse(content={'top_coins': top, 'count': len(top)})
        
        @self.app.get("/api/status")
        async def get_status():
            uptime = (datetime.now() - self.startup_time).total_seconds()
            return JSONResponse(content={
                'running': self.running,
                'uptime_seconds': uptime,
                'last_scan': self.last_scan.isoformat() if self.last_scan else None,
                'scan_count': self.scan_count,
                'coins_tracked': len(self.analyzer.coins),
                'cpu_percent': psutil.cpu_percent(),
                'memory_percent': psutil.virtual_memory().percent
            })
        
        @self.app.post("/api/scan/trigger")
        async def trigger_scan():
            asyncio.create_task(self._run_scan())
            return JSONResponse(content={'status': 'scan_triggered'})
        
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.websockets.add(websocket)
            try:
                await websocket.send_json({'type': 'connected', 'message': 'Connected to analyzer'})
                while True:
                    await asyncio.sleep(30)
                    await websocket.send_json({'type': 'heartbeat'})
            except WebSocketDisconnect:
                self.websockets.discard(websocket)
    
    async def _broadcast(self, message: Dict):
        disconnected = set()
        for ws in self.websockets:
            try:
                await ws.send_json(message)
            except:
                disconnected.add(ws)
        self.websockets -= disconnected
    
    async def _run_scan(self):
        logger.info("Starting market scan...")
        try:
            results = await self.analyzer.scan_all()
            self.scan_results = results
            self.last_scan = datetime.now()
            self.scan_count += 1
            top_coins = [r for r in results if r['score']['percentage'] >= 75]
            logger.info(f"Scan #{self.scan_count} complete: {len(results)} coins, {len(top_coins)} high score")
            await self._broadcast({
                'type': 'scan_complete',
                'scan_count': self.scan_count,
                'coins_analyzed': len(results),
                'top_coins': len(top_coins)
            })
        except Exception as e:
            logger.error(f"Scan error: {e}")
    
    async def scan_loop(self):
        while self.running:
            await self._run_scan()
            await asyncio.sleep(self.scan_interval)
    
    def _get_dashboard_html(self) -> str:
        try:
            with open('templates/dashboard.html', 'r') as f:
                return f.read()
        except:
            return "<html><body><h1>Dashboard template not found</h1></body></html>"


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
    logger.info("Market Analyzer started")

@app.on_event("shutdown")
async def shutdown():
    get_app().running = False
    logger.info("Market Analyzer stopped")

if __name__ == "__main__":
    os.makedirs("templates", exist_ok=True)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False, log_level="info")