"""
backtest_web.py — Backtest strategi EMA-Cross Reversal + Flip Protection (H1)
==============================================================================
Semua coin, data H1 live dari Bybit API.

Strategi (hasil riset & backtest terbaik, lihat Readme.md):
  1. Deteksi support/resistance valid (basis body candle H1, strict + validasi
     "level sebelumnya masih hidup").
  2. Arah DIBALIK: support valid -> bias SHORT, resistance valid -> bias LONG.
     Bias tetap hidup untuk re-entry berulang sampai ada S/R valid baru.
  3. Entry: LIMIT order di WICK candle yang menyebabkan EMA cross (EMA4/EMA10).
     SL = wick diperpanjang sejauh jarak (wick, close candle cross) ke arah berlawanan.
  4. FLIP PROTECTION: EMA cross berlawanan muncul saat pending/aktif -> batal/tutup
     SEKARANG, apapun P&L-nya. Bias tetap hidup, tunggu cross searah lagi.
  5. Trailing stop aktif di rasio 1:TRAIL_ACT_R (default 6) dari jarak entry-SL.

Deploy ke Railway:
  Start command -> python backtest_web.py
  Buka domain Railway -> lihat progress & hasil di browser (auto-refresh)
"""

import os, threading, time, io, csv
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import numpy as np
import pandas as pd
from pybit.unified_trading import HTTP

# ============================================================
# CONFIG (override via environment variable kalau perlu)
# ============================================================
PORT             = int(os.environ.get('PORT', 8080))
INITIAL_BALANCE  = float(os.environ.get('INITIAL_BALANCE', '30.0'))   # modal awal, 1 AKUN BERSAMA (bukan per coin)
RISK_PCT         = float(os.environ.get('RISK_PCT', '0.01'))          # risk 1% balance/trade (compound)
FEE_PCT          = float(os.environ.get('FEE_PCT', '0.00055'))        # taker fee per sisi (Bybit USDT perp)
EMA_FAST         = int(os.environ.get('EMA_FAST', '4'))
EMA_SLOW         = int(os.environ.get('EMA_SLOW', '10'))
TRAIL_ACT_R      = float(os.environ.get('TRAIL_ACT_R', '6.0'))        # trailing aktif di rasio 1:6
TRAIL_STOP       = float(os.environ.get('TRAIL_STOP', '1.0'))         # lebar trailing = 1x dist
MIN_DIST_PCT     = float(os.environ.get('MIN_DIST_PCT', '0.002'))     # floor SL minimum 0.2%

# Rentang backtest: FIX (bukan "N hari terakhir") supaya data bisa di-cache & tidak
# perlu fetch ulang dari Bybit tiap kali variable diubah. Format: YYYY-MM-DD.
BACKTEST_START_DATE = os.environ.get('BACKTEST_START_DATE', '2025-08-01')
BACKTEST_END_DATE   = os.environ.get('BACKTEST_END_DATE', '2026-07-31')

# LEVERAGE & MARGIN: constraint paling realistis dari exchange asli. Risk 1% BUKAN berarti
# ada "99 kesempatan lagi" -- tiap posisi tetap butuh MARGIN (notional/leverage), dan kalau
# margin yg dipakai SEMUA posisi terbuka sudah habis, Bybit tidak akan izinkan order baru
# sampai ada yg closed. Constraint inilah yg secara alami membatasi berapa banyak posisi
# bisa dibuka bersamaan -- BUKAN cuma persentase risiko semata.
LEVERAGE           = float(os.environ.get('LEVERAGE', '25'))
MARGIN_USAGE_CAP    = float(os.environ.get('MARGIN_USAGE_CAP', '0.90'))   # max 90% balance dipakai jd margin

# MAX_CONCURRENT: default TANPA BATAS (seperti sebelumnya). Isi angka di Railway
# Variables (mis. MAX_CONCURRENT=10) kalau mau membatasi slot global lagi.
# 0 / kosong / 'unlimited' = tanpa batas. Dengan constraint MARGIN di atas, batas alami
# akan tetap muncul dari margin habis, bukan cuma dari MAX_CONCURRENT.
_mc_raw = os.environ.get('MAX_CONCURRENT', '0').strip().lower()
MAX_CONCURRENT = float('inf') if _mc_raw in ('', '0', 'unlimited', 'inf') else int(_mc_raw)

ALLOW_HEDGE      = os.environ.get('ALLOW_HEDGE', 'true').lower() == 'true'  # Long & Short boleh bareng per koin

# FILTER berbasis indikator saat cross. Default AKTIF di ATR ratio >= 1.0 (candle cross harus
# minimal sebesar volatilitas normalnya) -- dari hasil analisis, ini indikator dgn spread WR
# paling konsisten & kuat (34.7% -> 47.2%). Override / matikan (isi 0) lewat Railway Variables.
FILTER_MIN_ATR_RATIO   = float(os.environ.get('FILTER_MIN_ATR_RATIO', '1.0'))    # 0 = nonaktif
FILTER_MIN_VOL_RATIO   = float(os.environ.get('FILTER_MIN_VOL_RATIO', '0'))      # 0 = nonaktif
FILTER_MAX_EMA_GAP_PCT = float(os.environ.get('FILTER_MAX_EMA_GAP_PCT', '0'))    # 0 = nonaktif

# CACHE data candle H1 ke disk supaya tidak perlu fetch ulang dari Bybit tiap kali variable
# strategi diubah. Arahkan CACHE_DIR ke mount point Railway Volume (mis. /data/cache) biar
# persisten lintas redeploy. Tanpa Volume, cache tetap jalan tapi hilang tiap redeploy.
CACHE_DIR = os.environ.get('CACHE_DIR', './data_cache')
os.makedirs(CACHE_DIR, exist_ok=True)

SYMBOLS = [
    'XPLUSDT', 'MNTUSDT', 'PLUMEUSDT', 'HYPEUSDT', 'BNBUSDT', 'BELUSDT', 'BERAUSDT', 'DASHUSDT',
    'DOGEUSDT', 'USUALUSDT', 'TAOUSDT', 'ESPORTSUSDT', 'LABUSDT', 'HUSDT', 'AVAXUSDT', 'REUSDT',
    '1000BONKUSDT', 'ORCAUSDT', 'AAVEUSDT', 'GMXUSDT', 'LTCUSDT', 'ICPUSDT', 'VIRTUALUSDT', 'CFXUSDT',
    'UNIUSDT', 'ONDOUSDT', 'SUIUSDT', 'ALGOUSDT', 'HBARUSDT', 'EIGENUSDT', 'XRPUSDT', 'SOLUSDT',
    'CRVUSDT', 'RENDERUSDT', 'XVGUSDT', 'SANDUSDT', 'AXSUSDT', 'IMXUSDT', 'FARTCOINUSDT', 'OPUSDT',
    '1000PEPEUSDT', 'TIAUSDT', 'GALAUSDT', 'APEUSDT', 'FLOWUSDT',
]

def _date_to_ms(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

_START_MS = _date_to_ms(BACKTEST_START_DATE)
_END_MS   = _date_to_ms(BACKTEST_END_DATE) + 86400 * 1000 - 1   # sampai akhir hari BACKTEST_END_DATE

# ============================================================
# GLOBAL STATE (dibaca oleh HTTP handler, ditulis oleh background thread)
# ============================================================
_lock       = threading.Lock()
_log        = []
_phase      = 'running'     # running | done | error
_results    = []            # list per-coin dict
_all_trades = []            # semua trade, semua coin (utk CSV & agregat)
_combined_result = {         # ringkasan hasil simulasi gabungan (1 balance, 1 pool slot)
    'n_trades': 0, 'n_win': 0, 'n_loss': 0, 'wr': 0, 'total_pnl': 0, 'roi': 0,
    'total_r': 0, 'avg_r': 0, 'final_balance': INITIAL_BALANCE,
    'blocked_by_slot': 0, 'blocked_by_margin': 0, 'blocked_by_filter': 0,
}
_indicator_result = {}      # hasil analisis indikator saat cross (win vs loss)


def _ts():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%H:%M:%S')

def _log_msg(msg: str):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    with _lock:
        _log.append(line)


# ============================================================
# FETCH DATA H1 DARI BYBIT (dgn CACHE ke disk supaya tak fetch ulang tiap kali)
# ============================================================

def _cache_path(symbol: str) -> str:
    # nama file menyertakan rentang tanggal -> otomatis fetch ulang kalau rentang berubah
    return os.path.join(CACHE_DIR, f"{symbol}_{BACKTEST_START_DATE}_{BACKTEST_END_DATE}.csv")


def _load_cache(symbol: str):
    path = _cache_path(symbol)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if not df.empty and {'ts', 'open', 'high', 'low', 'close', 'vol'}.issubset(df.columns):
                return df
        except Exception as e:
            _log_msg(f"   ⚠ {symbol}: cache korup ({e}), fetch ulang dari Bybit.")
    return None


def _save_cache(symbol: str, df: pd.DataFrame):
    try:
        df.to_csv(_cache_path(symbol), index=False)
    except Exception as e:
        _log_msg(f"   ⚠ {symbol}: gagal simpan cache — {e}")


def fetch_bybit_h1(symbol: str) -> pd.DataFrame:
    cached = _load_cache(symbol)
    if cached is not None:
        _log_msg(f"   💾 {symbol}: pakai cache ({len(cached):,} candle, {BACKTEST_START_DATE} s/d {BACKTEST_END_DATE}) — skip fetch Bybit.")
        return cached

    session = HTTP(testnet=False)
    rows, cur_end, n_call = [], _END_MS, 0
    while True:
        for attempt in range(4):
            try:
                res = session.get_kline(symbol=symbol, category='linear', interval=60,
                                        limit=1000, start=_START_MS, end=cur_end)
                data = res['result']['list']
                break
            except Exception as e:
                wait = 2 ** attempt
                _log_msg(f"   ⚠ {symbol} API error (attempt {attempt+1}): {e} — retry {wait}s")
                time.sleep(wait)
        else:
            _log_msg(f"   ❌ {symbol}: gagal fetch setelah 4 percobaan.")
            break
        if not data:
            break
        for kl in data:
            rows.append({'ts': int(kl[0]), 'open': float(kl[1]), 'high': float(kl[2]),
                         'low': float(kl[3]), 'close': float(kl[4]), 'vol': float(kl[5])})
        n_call += 1
        oldest_ts = int(data[-1][0])
        if oldest_ts <= _START_MS:
            break
        cur_end = oldest_ts - 1
        time.sleep(0.15)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates(subset='ts').sort_values('ts').reset_index(drop=True)
    _save_cache(symbol, df)
    return df


# ============================================================
# DETEKSI SUPPORT / RESISTANCE (identik dgn bot_ema_flip.py)
# ============================================================

def find_sr_events(df):
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    ts = df['ts'].values
    n = len(df)
    raw = []
    for i in range(0, n - 2):
        if c[i] < o[i] and c[i + 1] > o[i + 1]:
            S = c[i]
            if l[i + 2] > S + 1e-9:
                raw.append({'type': 'support', 'level': S, 'sl': min(l[i], l[i + 1]),
                            'c1': i, 'c2': i + 1, 'c3': i + 2, 'c1_ts': int(ts[i])})
        if c[i] > o[i] and c[i + 1] < o[i + 1]:
            R = c[i]
            if h[i + 2] < R - 1e-9:
                raw.append({'type': 'resistance', 'level': R, 'sl': max(h[i], h[i + 1]),
                            'c1': i, 'c2': i + 1, 'c3': i + 2, 'c1_ts': int(ts[i])})
    raw.sort(key=lambda e: e['c3'])
    stack = {'support': [], 'resistance': []}
    events = []
    for e in raw:
        ty = e['type']; cutoff = e['c1']
        alive = []
        for ref in stack[ty]:
            broken = False
            for j in range(ref['c3'] + 1, cutoff + 1):
                if ty == 'support' and c[j] < ref['sl'] - 1e-12:
                    broken = True; break
                if ty == 'resistance' and c[j] > ref['sl'] + 1e-12:
                    broken = True; break
            if not broken:
                alive.append(ref)
        stack[ty] = alive
        prev = stack[ty][-1]['level'] if stack[ty] else None
        wick_extreme = e['sl']; S = e['level']
        if prev is None:
            e['valid'] = False
        elif ty == 'support':
            e['valid'] = (wick_extreme <= prev + 1e-12) and (prev <= S + 1e-12)
        else:
            e['valid'] = (wick_extreme >= prev - 1e-12) and (prev >= S - 1e-12)
        events.append(e)
        stack[ty].append({'level': S, 'sl': wick_extreme, 'c3': e['c3']})
    return events


# ============================================================
# PERSIAPAN PER-KOIN (precompute EMA + support/resistance events + indikator)
# ============================================================

VOL_MA_PERIOD  = int(os.environ.get('VOL_MA_PERIOD', '20'))   # rata-rata volume utk hitung rasio
ATR_PERIOD     = int(os.environ.get('ATR_PERIOD', '14'))
EMA_TREND      = int(os.environ.get('EMA_TREND', '50'))       # EMA konteks tren (filter arah besar)

def _calc_atr(H, L, C, period):
    prev_close = np.roll(C, 1)
    prev_close[0] = C[0]
    tr = np.maximum(H - L, np.maximum(np.abs(H - prev_close), np.abs(L - prev_close)))
    return pd.Series(tr).rolling(period, min_periods=1).mean().values


# ============================================================
# INDIKATOR SAAT CROSS (utk analisis pola menang/kalah)
# ============================================================

def _capture_indicators(c, i):
    """Ambil snapshot indikator persis di candle i (candle penyebab EMA cross)."""
    close_i = c['C'][i]
    vol_ma  = c['vol_ma'][i]
    atr_i   = c['atr'][i]
    rng     = c['H'][i] - c['L'][i]
    return {
        'vol_ratio': (c['V'][i] / vol_ma) if vol_ma > 0 else None,          # volume vs rata2 20 candle
        'atr_ratio': (rng / atr_i) if atr_i > 0 else None,                  # besar candle vs volatilitas normal
        'ema_gap_pct': (abs(c['ema_fast'][i] - c['ema_slow'][i]) / close_i * 100) if close_i else None,
        'trend_pct': ((close_i - c['ema_trend'][i]) / c['ema_trend'][i] * 100) if c['ema_trend'][i] else None,
        'dist_pct': None,   # diisi setelah dist final diketahui (lihat di bawah)
    }


def prepare_coin(symbol, df):
    """Precompute semua yang dibutuhkan simulasi + indikator (utk analisis win/loss) utk 1 koin.
    None kalau data kurang."""
    n = len(df)
    warmup = max(EMA_SLOW, EMA_TREND, VOL_MA_PERIOD, ATR_PERIOD) + 10
    if n < warmup + 10:
        return None
    O = df['open'].values; H = df['high'].values; L = df['low'].values; C = df['close'].values
    V = df['vol'].values if 'vol' in df.columns else np.zeros(n)
    TS = df['ts'].values
    ema_fast = df['close'].ewm(span=EMA_FAST, adjust=False).mean().values
    ema_slow = df['close'].ewm(span=EMA_SLOW, adjust=False).mean().values
    ema_trend = df['close'].ewm(span=EMA_TREND, adjust=False).mean().values
    vol_ma = pd.Series(V).rolling(VOL_MA_PERIOD, min_periods=1).mean().values
    atr = _calc_atr(H, L, C, ATR_PERIOD)
    events = find_sr_events(df)
    events_by_c3 = {}
    for e in events:
        events_by_c3.setdefault(e['c3'], []).append(e)
    return {
        'symbol': symbol, 'O': O, 'H': H, 'L': L, 'C': C, 'V': V, 'TS': TS,
        'ema_fast': ema_fast, 'ema_slow': ema_slow, 'ema_trend': ema_trend,
        'vol_ma': vol_ma, 'atr': atr, 'events_by_c3': events_by_c3,
        'n': n, 'warmup': warmup,
        'ts_to_idx': {int(TS[i]): i for i in range(n)},
    }


# ============================================================
# SIMULASI GABUNGAN — SEMUA KOIN BERBARENGAN, 1 BALANCE (COMPOUNDING),
# 1 POOL MAX_CONCURRENT (persis seperti bot live: satu akun, slot terbatas
# dipakai bersama oleh semua koin, bukan simulasi per-koin terisolasi)
# ============================================================

def run_combined_backtest(coins: dict) -> dict:
    """coins: {symbol: prepared_dict dari prepare_coin()}"""
    # ── timeline global: semua timestamp dari semua koin, urut kronologis ──
    all_ts = set()
    for c in coins.values():
        all_ts.update(int(t) for t in c['TS'][c['warmup']: c['n'] - 1])
    timeline = sorted(all_ts)

    balance = INITIAL_BALANCE
    armed             = {}   # f"{symbol}|Short"/"Long" -> {'c1_ts'}
    pending           = {}   # f"{symbol}|Long"/"Short" -> {...}
    active_positions  = {}   # f"{symbol}|Long"/"Short" -> {...}
    trades            = []
    blocked_by_slot   = 0    # counter: berapa kali sinyal valid terpaksa dilewati krn slot penuh
    blocked_by_margin = 0    # counter: dilewati krn margin (leverage) sudah habis -- constraint ASLI Bybit
    blocked_by_filter = 0    # counter: dilewati krn tidak lolos filter indikator

    def _akey(symbol, direction):
        return f"{symbol}|{direction}" if ALLOW_HEDGE else symbol

    def _slots_used():
        return len(active_positions) + len(pending)

    def _current_margin_used():
        """Total margin yg sedang dipakai SEMUA posisi terbuka (notional/leverage) --
        persis seperti akun Bybit riil, ini yg membatasi berapa banyak posisi bisa
        dibuka bersamaan, BUKAN sekadar persentase risiko."""
        return sum((p['entry'] * p['qty']) / LEVERAGE for p in active_positions.values())

    def _passes_filters(ind):
        if FILTER_MIN_ATR_RATIO > 0 and (ind.get('atr_ratio') or 0) < FILTER_MIN_ATR_RATIO:
            return False
        if FILTER_MIN_VOL_RATIO > 0 and (ind.get('vol_ratio') or 0) < FILTER_MIN_VOL_RATIO:
            return False
        if FILTER_MAX_EMA_GAP_PCT > 0 and (ind.get('ema_gap_pct') or 999) > FILTER_MAX_EMA_GAP_PCT:
            return False
        return True

    def close_trade(symbol, direction, exit_price, reason, exit_ts):
        nonlocal balance
        key = _akey(symbol, direction)
        pos = active_positions[key]
        entry, dist, qty = pos['entry'], pos['dist'], pos['qty']
        pnl_gross = (exit_price - entry) * qty if direction == 'Long' else (entry - exit_price) * qty
        fee = (entry * qty + exit_price * qty) * FEE_PCT
        pnl_net = pnl_gross - fee
        balance += pnl_net
        r_mult = pnl_gross / (dist * qty) if dist * qty else 0
        trade = {
            'symbol': symbol, 'direction': direction, 'entry': entry, 'sl': pos['sl'],
            'exit': exit_price, 'reason': reason, 'r_mult': r_mult, 'pnl_usd': pnl_net,
            'entry_ts': pos['entry_ts'], 'exit_ts': exit_ts, 'balance_after': balance,
        }
        trade.update(pos.get('ind') or {})
        trades.append(trade)
        del active_positions[key]

    n_ts = len(timeline)
    for step, ts in enumerate(timeline):
        if step % 500 == 0:
            _log_msg(f"   ⏱️  Simulasi gabungan: {step}/{n_ts} timestamp | "
                      f"balance ${balance:.2f} | slot {_slots_used()}/{MAX_CONCURRENT} | trade {len(trades)}")

        for symbol, c in coins.items():
            idx = c['ts_to_idx'].get(ts)
            if idx is None or idx < c['warmup'] or idx >= c['n'] - 1:
                continue   # koin ini tidak punya candle di jam ini, atau di luar rentang valid
            i = idx
            O, H, L, C_, TS = c['O'], c['H'], c['L'], c['C'], c['TS']
            ema_fast, ema_slow = c['ema_fast'], c['ema_slow']

            death_cross  = ema_fast[i-1] >= ema_slow[i-1] and ema_fast[i] < ema_slow[i]
            golden_cross = ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]

            key_long  = _akey(symbol, 'Long')
            key_short = _akey(symbol, 'Short')

            # ── 1) FLIP PROTECTION ──
            if death_cross and key_long in active_positions:
                close_trade(symbol, 'Long', O[i+1], 'FLIP', int(TS[i+1]))
            if death_cross:
                pending.pop(key_long, None)
            if golden_cross and key_short in active_positions:
                close_trade(symbol, 'Short', O[i+1], 'FLIP', int(TS[i+1]))
            if golden_cross:
                pending.pop(key_short, None)

            # ── 2) SL / trailing normal ──
            for direction, key in (('Short', key_short), ('Long', key_long)):
                pos = active_positions.get(key)
                if pos is None:
                    continue
                h, l = H[i], L[i]
                if direction == 'Long':
                    if l <= pos['stop']:
                        reason = 'TRAIL' if pos['trail_active'] else 'SL'
                        close_trade(symbol, 'Long', pos['stop'], reason, int(TS[i]))
                        continue
                    pos['peak'] = max(pos['peak'], h)
                    if not pos['trail_active'] and pos['peak'] >= pos['act_price']:
                        pos['trail_active'] = True
                    if pos['trail_active']:
                        pos['stop'] = max(pos['stop'], pos['peak'] - TRAIL_STOP * pos['dist'])
                else:
                    if h >= pos['stop']:
                        reason = 'TRAIL' if pos['trail_active'] else 'SL'
                        close_trade(symbol, 'Short', pos['stop'], reason, int(TS[i]))
                        continue
                    pos['peak'] = min(pos['peak'], l)
                    if not pos['trail_active'] and pos['peak'] <= pos['act_price']:
                        pos['trail_active'] = True
                    if pos['trail_active']:
                        pos['stop'] = min(pos['stop'], pos['peak'] + TRAIL_STOP * pos['dist'])

            # ── 3) cek fill pending (limit di wick), TUNDUK ke MARGIN (leverage) ──
            for direction, key in (('Short', key_short), ('Long', key_long)):
                p = pending.get(key)
                if p is not None and key not in active_positions:
                    filled = (H[i] >= p['entry']) if direction == 'Short' else (L[i] <= p['entry'])
                    if filled:
                        dist = p['dist']
                        min_dist = p['entry'] * MIN_DIST_PCT
                        if dist < min_dist:
                            dist = min_dist
                        risk_usd = balance * RISK_PCT   # <-- COMPOUNDING: 1% dari balance TERKINI (shared)
                        qty = risk_usd / dist if dist > 0 else 0

                        # MARGIN CHECK (persis Bybit asli): notional = qty*entry, margin = notional/leverage.
                        # Kalau margin yg sudah dipakai + margin posisi baru ini > batas (mis. 90% balance),
                        # order DITOLAK exchange -- risk 1% tidak berarti "99 kesempatan lagi", karena
                        # margin-nya sendiri yg akan habis duluan jauh sebelum itu.
                        margin_needed = (p['entry'] * qty) / LEVERAGE
                        if _current_margin_used() + margin_needed > balance * MARGIN_USAGE_CAP:
                            blocked_by_margin += 1
                            del pending[key]
                            continue

                        act_price = (p['entry'] + TRAIL_ACT_R * dist) if direction == 'Long' \
                                    else (p['entry'] - TRAIL_ACT_R * dist)
                        ind = dict(p.get('ind') or {})
                        ind['dist_pct'] = (dist / p['entry'] * 100) if p['entry'] else None
                        active_positions[key] = {
                            'entry': p['entry'], 'sl': p['sl'], 'dist': dist, 'stop': p['sl'],
                            'trail_active': False, 'peak': p['entry'], 'act_price': act_price,
                            'qty': qty, 'entry_ts': int(TS[i]), 'ind': ind,
                        }
                        del pending[key]

            # ── 4) daftarkan support/resistance valid baru -> armed (bias arah, tetap hidup) ──
            for e in c['events_by_c3'].get(i, []):
                if not e['valid']:
                    continue
                if e['type'] == 'support':
                    armed[key_short] = {'c1_ts': e['c1_ts']}
                else:
                    armed[key_long] = {'c1_ts': e['c1_ts']}

            # ── 5) cross SEARAH -> pasang/ganti limit di wick, TUNDUK ke MAX_CONCURRENT & FILTER ──
            # Tangkap indikator PERSIS di candle cross ini -> dipakai jg utk filter & analisis win/loss.
            if death_cross and key_short in armed and key_short not in active_positions:
                wick = H[i]; old_dist = wick - C_[i]
                if old_dist > 0:
                    ind = _capture_indicators(c, i)
                    if not _passes_filters(ind):
                        blocked_by_filter += 1
                    elif key_short not in pending and _slots_used() >= MAX_CONCURRENT:
                        blocked_by_slot += 1
                    else:
                        pending[key_short] = {
                            'entry': wick, 'sl': wick + old_dist, 'dist': old_dist, 'ind': ind,
                        }

            if golden_cross and key_long in armed and key_long not in active_positions:
                wick = L[i]; old_dist = C_[i] - wick
                if old_dist > 0:
                    ind = _capture_indicators(c, i)
                    if not _passes_filters(ind):
                        blocked_by_filter += 1
                    elif key_long not in pending and _slots_used() >= MAX_CONCURRENT:
                        blocked_by_slot += 1
                    else:
                        pending[key_long] = {
                            'entry': wick, 'sl': wick - old_dist, 'dist': old_dist, 'ind': ind,
                        }

    wins = [t for t in trades if t['r_mult'] > 0]
    total_pnl = sum(t['pnl_usd'] for t in trades)
    return {
        'trades': trades, 'final_balance': balance,
        'blocked_by_slot': blocked_by_slot, 'blocked_by_margin': blocked_by_margin,
        'blocked_by_filter': blocked_by_filter,
        'n_trades': len(trades), 'n_win': len(wins), 'n_loss': len(trades) - len(wins),
        'wr': (len(wins) / len(trades) * 100) if trades else 0,
        'total_pnl': total_pnl,
        'roi': (total_pnl / INITIAL_BALANCE * 100) if INITIAL_BALANCE else 0,
        'total_r': sum(t['r_mult'] for t in trades),
        'avg_r': (sum(t['r_mult'] for t in trades) / len(trades)) if trades else 0,
    }


def per_symbol_breakdown(trades):
    """Ringkas per-koin dari trade list gabungan (utk tabel per-coin di dashboard)."""
    by_sym = {}
    for t in trades:
        by_sym.setdefault(t['symbol'], []).append(t)
    out = []
    for sym, ts_ in by_sym.items():
        wins = [t for t in ts_ if t['r_mult'] > 0]
        pnl  = sum(t['pnl_usd'] for t in ts_)
        out.append({
            'symbol': sym, 'status': 'ok', 'n_trades': len(ts_), 'n_win': len(wins),
            'n_loss': len(ts_) - len(wins), 'wr': (len(wins) / len(ts_) * 100) if ts_ else 0,
            'total_pnl': pnl, 'roi': None,
            'avg_r': (sum(t['r_mult'] for t in ts_) / len(ts_)) if ts_ else 0,
        })
    return out


# ============================================================
# ANALISIS INDIKATOR SAAT CROSS — pola apa yg cenderung menang/kalah
# ============================================================

INDICATOR_INFO = {
    'vol_ratio':   {'label': 'Volume saat cross (vs rata² 20 candle)', 'fmt': '{:.2f}x'},
    'atr_ratio':   {'label': 'Ukuran candle cross (vs ATR14)',         'fmt': '{:.2f}x'},
    'ema_gap_pct': {'label': 'Jarak EMA4-EMA10 saat cross',            'fmt': '{:.3f}%'},
    'trend_pct':   {'label': 'Posisi close vs EMA50 (tren besar)',     'fmt': '{:+.2f}%'},
    'dist_pct':    {'label': 'Jarak SL dari entry',                    'fmt': '{:.3f}%'},
}

def indicator_analysis(trades):
    """Utk tiap indikator: avg saat WIN vs LOSS, + win rate per bucket (Rendah/Sedang/Tinggi)."""
    out = {}
    for key, info in INDICATOR_INFO.items():
        pairs = [(t[key], t['r_mult'] > 0) for t in trades if t.get(key) is not None]
        if len(pairs) < 6:
            continue
        win_vals  = [v for v, w in pairs if w]
        loss_vals = [v for v, w in pairs if not w]
        vals_sorted = sorted(v for v, _ in pairs)
        n = len(vals_sorted)
        t1 = vals_sorted[n // 3]
        t2 = vals_sorted[(2 * n) // 3]
        buckets = {'Rendah': [], 'Sedang': [], 'Tinggi': []}
        for v, w in pairs:
            if v <= t1:
                buckets['Rendah'].append(w)
            elif v <= t2:
                buckets['Sedang'].append(w)
            else:
                buckets['Tinggi'].append(w)
        out[key] = {
            'label': info['label'], 'fmt': info['fmt'],
            'avg_win': (sum(win_vals) / len(win_vals)) if win_vals else None,
            'avg_loss': (sum(loss_vals) / len(loss_vals)) if loss_vals else None,
            'n': len(pairs), 't1': t1, 't2': t2,
            'buckets': {
                name: {
                    'wr': (sum(ws) / len(ws) * 100) if ws else None,
                    'n': len(ws),
                } for name, ws in buckets.items()
            },
        }
    return out


# ============================================================
# BACKGROUND RUNNER
# ============================================================

def _fmt_max_concurrent():
    return "TANPA BATAS" if MAX_CONCURRENT == float('inf') else str(int(MAX_CONCURRENT))


def _run():
    global _phase, _combined_result, _indicator_result
    _log_msg(f"🚀 Mulai backtest GABUNGAN {len(SYMBOLS)} coin | H1 | {BACKTEST_START_DATE} s/d {BACKTEST_END_DATE}")
    _log_msg(f"   EMA {EMA_FAST}/{EMA_SLOW} | Trail 1:{TRAIL_ACT_R:.0f} | Risk {RISK_PCT*100:.0f}%/trade "
              f"(compounding, 1 balance bersama) | MAX_CONCURRENT {_fmt_max_concurrent()} slot (global, semua koin) | "
              f"Modal awal ${INITIAL_BALANCE:.0f}")

    coins = {}
    for sym in SYMBOLS:
        try:
            _log_msg(f"📥 {sym}: mengambil data H1 dari Bybit...")
            df = fetch_bybit_h1(sym)
            if df.empty:
                _log_msg(f"   ⚠ {sym}: data kosong, skip.")
                continue
            prepared = prepare_coin(sym, df)
            if prepared is None:
                _log_msg(f"   ⚠ {sym}: data kurang ({len(df)} candle), skip.")
                continue
            coins[sym] = prepared
            _log_msg(f"   {len(df):,} candle H1 diperoleh & siap.")
        except Exception as e:
            _log_msg(f"   ❌ {sym}: error fetch — {e}")

    if not coins:
        _log_msg("❌ Tidak ada data koin sama sekali, backtest dibatalkan.")
        with _lock:
            _phase = 'error'
        return

    _log_msg(f"✅ {len(coins)}/{len(SYMBOLS)} koin siap. Menjalankan SIMULASI GABUNGAN "
              f"(semua koin berbarengan sesuai waktu, 1 balance, {_fmt_max_concurrent()} slot global)...")

    result = run_combined_backtest(coins)
    ind_analysis = indicator_analysis(result['trades'])

    with _lock:
        _combined_result.update(result)
        _all_trades.extend(result['trades'])
        _results.extend(per_symbol_breakdown(result['trades']))
        _indicator_result.update(ind_analysis)
        _phase = 'done'

    _log_msg(f"🏁 Selesai! {result['n_trades']} trade | WR {result['wr']:.1f}% | "
              f"PnL ${result['total_pnl']:+.2f} | ROI {result['roi']:+.1f}% | "
              f"Balance akhir ${result['final_balance']:.2f} | "
              f"blocked: {result['blocked_by_slot']} (slot), {result['blocked_by_margin']} (margin), "
              f"{result['blocked_by_filter']} (filter indikator)")


# ============================================================
# HTML RENDERING
# ============================================================

_CSS = '''
<style>
  body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,Segoe UI,Roboto,sans-serif;
       margin:0;padding:16px 20px 60px}
  h1{font-size:20px;margin:0 0 6px}
  h2{font-size:15px;margin:22px 0 8px;color:#58a6ff}
  p{font-size:13px;line-height:1.6}
  table{border-collapse:collapse;width:100%;font-size:12px}
  th,td{padding:6px 10px;border-bottom:1px solid #21262d;text-align:right}
  th:first-child,td:first-child{text-align:left}
  th{color:#8b949e;font-weight:600;background:#161b22;position:sticky;top:0}
  tr:hover{background:#161b22}
  .g{color:#3fb950}
  .r{color:#f85149}
  .y{color:#d29922}
  .chip{display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600}
  .chip.running{background:#1f6feb33;color:#58a6ff}
  .chip.done{background:#23863633;color:#3fb950}
  .chip.error{background:#f8514933;color:#f85149}
  .tbl-wrap{overflow-x:auto;border:1px solid #21262d;border-radius:6px}
  .log{background:#010409;border:1px solid #21262d;border-radius:6px;padding:10px 14px;
       font-family:'Courier New',monospace;font-size:11px;max-height:360px;overflow-y:auto;
       white-space:pre-wrap}
  .note{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 14px;
        font-size:12px;color:#8b949e;margin-top:10px}
  a{color:#58a6ff}
  .dlbtn{display:inline-block;margin:2px;padding:4px 10px;background:#21262d;border-radius:5px;
         font-size:11px;text-decoration:none;color:#c9d1d9}
  .dlbtn:hover{background:#30363d}
</style>
'''

def _render_html() -> bytes:
    with _lock:
        phase   = _phase
        res_cp  = list(_results)
        log_cp  = list(_log)
        trades_cp = list(_all_trades)
        cr = dict(_combined_result)
        ind_cp = dict(_indicator_result)

    refresh = '<meta http-equiv="refresh" content="5">' if phase == 'running' else ''
    chip_cls = {'running': 'running', 'done': 'done', 'error': 'error'}.get(phase, 'running')
    chip_txt = {'running': '⏳ Sedang berjalan...', 'done': '✅ Selesai', 'error': '❌ Error'}.get(phase, phase)

    n_done, n_total = len(res_cp), len(SYMBOLS)

    # ── ringkasan gabungan: dari SIMULASI GABUNGAN (1 balance, 1 pool slot), bukan jumlah per-coin ──
    total_trades = cr.get('n_trades', 0)
    total_win    = cr.get('n_win', 0)
    total_pnl    = cr.get('total_pnl', 0)
    wr_overall   = cr.get('wr', 0)
    avg_r        = cr.get('avg_r', 0)
    roi_overall  = cr.get('roi', 0)
    final_bal    = cr.get('final_balance', INITIAL_BALANCE)

    gross_win  = sum(t['pnl_usd'] for t in trades_cp if t['pnl_usd'] > 0)
    gross_loss = abs(sum(t['pnl_usd'] for t in trades_cp if t['pnl_usd'] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 0 else float('inf')

    blocked_slot   = cr.get('blocked_by_slot', 0)
    blocked_margin = cr.get('blocked_by_margin', 0)
    blocked_filter = cr.get('blocked_by_filter', 0)

    summary_html = f'''
    <h2>Ringkasan Gabungan — 1 balance, 1 pool slot (COMPOUNDING) ({n_done}/{n_total} coin dimuat)</h2>
    <table>
      <tr><th>Total Trade</th><th>Win</th><th>Loss</th><th>WR%</th><th>Total PnL</th>
          <th>ROI%</th><th>Balance Akhir</th><th>Avg R/trade</th><th>Profit Factor</th>
          <th>Blokir: Slot</th><th>Blokir: Margin</th><th>Blokir: Filter</th></tr>
      <tr>
        <td>{total_trades}</td>
        <td class="g">{total_win}</td>
        <td class="r">{total_trades-total_win}</td>
        <td class="{'g' if wr_overall>=40 else ('y' if wr_overall>=25 else 'r')}">{wr_overall:.1f}%</td>
        <td class="{'g' if total_pnl>=0 else 'r'}">${total_pnl:+,.2f}</td>
        <td class="{'g' if roi_overall>=0 else 'r'}">{roi_overall:+,.1f}%</td>
        <td>${final_bal:,.2f}</td>
        <td class="{'g' if avg_r>=0 else 'r'}">{avg_r:+.3f}</td>
        <td>{pf:.2f}</td>
        <td class="y">{blocked_slot}</td>
        <td class="y">{blocked_margin}</td>
        <td class="y">{blocked_filter}</td>
      </tr>
    </table>
    <p style="font-size:12px;color:#8b949e">Leverage: <b>{LEVERAGE:.0f}x</b> | Margin usage cap: maksimal
    <b>{MARGIN_USAGE_CAP*100:.0f}%</b> dari balance boleh dipakai sbg margin bersamaan (dari SEMUA posisi
    terbuka -- persis constraint margin Bybit asli, bukan cuma persentase risiko). Filter aktif: {
        ', '.join(f
        for f in [
            f'ATR≥{FILTER_MIN_ATR_RATIO}x' if FILTER_MIN_ATR_RATIO > 0 else '',
            f'Vol≥{FILTER_MIN_VOL_RATIO}x' if FILTER_MIN_VOL_RATIO > 0 else '',
            f'EMA gap≤{FILTER_MAX_EMA_GAP_PCT}%' if FILTER_MAX_EMA_GAP_PCT > 0 else '',
        ] if f
    ) or 'tidak ada (semua nonaktif)'}</p>
    '''

    # ── tabel per coin (kontribusi PnL masing-masing dari balance BERSAMA di atas) ──
    rows = ''
    for r in sorted(res_cp, key=lambda x: -(x.get('total_pnl', -1e18) if x.get('status')=='ok' else -1e18)):
        if r.get('status') != 'ok':
            rows += (f'<tr><td>{r["symbol"]}</td><td colspan="6" class="y">'
                      f'{r.get("reason","skip")}</td></tr>\n')
            continue
        pnl_c = 'g' if r['total_pnl'] >= 0 else 'r'
        wr_c  = 'g' if r['wr'] >= 40 else ('y' if r['wr'] >= 25 else 'r')
        rows += (
            f'<tr><td><b>{r["symbol"]}</b></td>'
            f'<td>{r["n_trades"]}</td>'
            f'<td class="g">{r["n_win"]}</td>'
            f'<td class="r">{r["n_loss"]}</td>'
            f'<td class="{wr_c}">{r["wr"]:.1f}%</td>'
            f'<td class="{pnl_c}">${r["total_pnl"]:+.2f}</td>'
            f'<td>{r["avg_r"]:+.3f}</td></tr>\n'
        )
    coin_table = f'''
    <table>
      <tr><th>Coin</th><th>Trade</th><th>Win</th><th>Loss</th><th>WR%</th>
          <th>Kontribusi PnL$</th><th>Avg R</th></tr>
      {rows or '<tr><td colspan="7" class="y">Menunggu hasil...</td></tr>'}
    </table>
    '''

    log_html = '\n'.join(log_cp[-400:])

    # ── tabel analisis indikator (win vs loss) ──
    ind_rows = ''
    for key, d in ind_cp.items():
        fmt = d['fmt']
        avg_win_s  = fmt.format(d['avg_win']) if d['avg_win'] is not None else '-'
        avg_loss_s = fmt.format(d['avg_loss']) if d['avg_loss'] is not None else '-'
        t1_s = fmt.format(d['t1'])
        t2_s = fmt.format(d['t2'])
        b = d['buckets']
        def _bucket_cell(name):
            info = b[name]
            if info['wr'] is None:
                return f'<td class="y">-</td>'
            cls = 'g' if info['wr'] >= 45 else ('y' if info['wr'] >= 30 else 'r')
            return f'<td class="{cls}">{info["wr"]:.1f}% <span style="color:#8b949e">(n={info["n"]})</span></td>'
        ind_rows += (
            f'<tr><td>{d["label"]}</td>'
            f'<td class="g">{avg_win_s}</td>'
            f'<td class="r">{avg_loss_s}</td>'
            f'{_bucket_cell("Rendah")}'
            f'{_bucket_cell("Sedang")}'
            f'{_bucket_cell("Tinggi")}'
            f'<td style="color:#8b949e">≤{t1_s} / ≤{t2_s}</td>'
            f'</tr>\n'
        )
    ind_table = f'''
    <table>
      <tr><th>Indikator (saat candle cross)</th><th>Avg saat WIN</th><th>Avg saat LOSS</th>
          <th>WR% - Rendah</th><th>WR% - Sedang</th><th>WR% - Tinggi</th>
          <th>Batas Rendah/Sedang</th></tr>
      {ind_rows or '<tr><td colspan="7" class="y">Belum ada data (minimal 6 trade per indikator).</td></tr>'}
    </table>
    ''' if ind_cp else '<p class="note">Belum ada data indikator.</p>'

    return f'''<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8">
  <title>Backtest EMA-Cross Reversal + Flip Protection</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {_CSS}
  {refresh}
</head>
<body>
  <h1>🤖 Backtest EMA-Cross Reversal + Flip Protection ({len(SYMBOLS)} coin, H1)</h1>
  <p>
    Modal awal: <b>${INITIAL_BALANCE:.0f}</b> (1 akun bersama, bukan per-coin) &nbsp;|&nbsp;
    Slot maksimum: <b>{_fmt_max_concurrent()}</b> (global, dipakai bersama semua koin) &nbsp;|&nbsp;
    Rentang: <b>{BACKTEST_START_DATE} s/d {BACKTEST_END_DATE}</b> &nbsp;|&nbsp;
    EMA <b>{EMA_FAST}/{EMA_SLOW}</b> &nbsp;|&nbsp;
    Trail aktif <b>1:{TRAIL_ACT_R:.0f}</b> &nbsp;|&nbsp;
    Status: <span class="chip {chip_cls}">{chip_txt}</span>
  </p>

  {summary_html}

  <h2>Kontribusi Per Coin</h2>
  <div class="tbl-wrap">{coin_table}</div>

  <h2>Analisis Indikator saat Cross — Pola Menang vs Kalah</h2>
  <div class="tbl-wrap">{ind_table}</div>
  <div class="note">
    💡 Cara baca: bandingkan kolom "Avg saat WIN" vs "Avg saat LOSS" — kalau beda jauh, indikator itu
    berpotensi jadi FILTER. Kolom WR%-Rendah/Sedang/Tinggi membagi SEMUA trade jadi 3 kelompok
    (tercile) berdasarkan nilai indikator itu saat cross, lalu tunjukkan win rate tiap kelompok.
    Kolom "Batas Rendah/Sedang" adalah nilai ambang aktual pembagi tercile (mis. "≤1.05x / ≤1.40x"
    artinya Rendah = sampai 1.05x, Sedang = 1.05x-1.40x, Tinggi = di atas 1.40x).
    <br>Kalau mau menerapkan filter berdasarkan hasil ini, isi env var di Railway:
    <code>FILTER_MIN_ATR_RATIO</code>, <code>FILTER_MIN_VOL_RATIO</code>, atau
    <code>FILTER_MAX_EMA_GAP_PCT</code> (nilai ambang batas Sedang/Tinggi di atas), lalu jalankan
    ulang backtest untuk lihat dampaknya ke Total R & WR keseluruhan (bukan cuma tercile).
  </div>

  <div class="note">
    💡 Entry = LIMIT di wick candle penyebab EMA cross. SL = wick diperpanjang sejauh
    jarak yang sama. Support valid → bias Short, Resistance valid → bias Long (arah dibalik).
    Flip protection: cross berlawanan → keluar/batal seketika, tunggu cross searah lagi.
    Trailing aktif di rasio 1:{TRAIL_ACT_R:.0f}, lebar {TRAIL_STOP:.1f}x dist.
    <br>⚙️ Risk {RISK_PCT*100:.0f}% dihitung dari balance TERKINI (compounding, 1 akun bersama —
    bukan modal terpisah per coin). Kalau slot ({MAX_CONCURRENT}) penuh saat sinyal valid baru
    muncul di koin lain, sinyal itu dilewati (lihat kolom "Sinyal Terblokir" di ringkasan).
    <br>Unduh semua trade: <a href="/trades.csv">/trades.csv</a> &nbsp;|&nbsp;
    Log mentah: <a href="/logs">/logs</a>
  </div>

  <h2>Log Progress</h2>
  <div class="log" id="log">{log_html}</div>
  <script>var e=document.getElementById('log');if(e)e.scrollTop=e.scrollHeight;</script>
</body>
</html>'''.encode('utf-8')


def _trades_csv() -> bytes:
    with _lock:
        trades_cp = list(_all_trades)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        'symbol', 'direction', 'entry', 'sl', 'exit', 'reason', 'r_mult',
        'pnl_usd', 'entry_ts', 'exit_ts', 'balance_after',
        'vol_ratio', 'atr_ratio', 'ema_gap_pct', 'trend_pct', 'dist_pct'],
        extrasaction='ignore')
    writer.writeheader()
    for t in trades_cp:
        writer.writerow(t)
    return buf.getvalue().encode('utf-8')


# ============================================================
# HTTP HANDLER
# ============================================================

class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/trades.csv':
            body = _trades_csv()
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="trades.csv"')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == '/logs':
            with _lock:
                body = '\n'.join(_log).encode('utf-8')
            ctype = 'text/plain; charset=utf-8'
        else:
            body = _render_html()
            ctype = 'text/html; charset=utf-8'

        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == '__main__':
    threading.Thread(target=_run, daemon=True).start()
    server = HTTPServer(('0.0.0.0', PORT), _Handler)
    print(f"🌐 Server running on port {PORT}", flush=True)
    server.serve_forever()
