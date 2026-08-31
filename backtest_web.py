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
     SL = SL_PCT (default 0.3%) dari entry (wick), arah berlawanan dari entry.
  4. FLIP PROTECTION: EMA cross berlawanan muncul saat pending/aktif -> batal/tutup
     SEKARANG, apapun P&L-nya. Bias tetap hidup, tunggu cross searah lagi.
  5. Trailing stop aktif di rasio 1:TRAIL_ACT_R (default 4) dari jarak entry-SL.

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
FEE_ENTRY_PCT    = float(os.environ.get('FEE_ENTRY_PCT', '0.00055'))   # fee saat BUKA posisi (taker, Bybit USDT perp)
FEE_EXIT_PCT     = float(os.environ.get('FEE_EXIT_PCT', str(0.00055 * 3)))  # fee saat TUTUP posisi = 3x fee entry
EMA_FAST         = int(os.environ.get('EMA_FAST', '4'))
EMA_SLOW         = int(os.environ.get('EMA_SLOW', '10'))
TRAIL_ACT_R      = float(os.environ.get('TRAIL_ACT_R', '4.0'))        # trailing aktif di rasio 1:4
TRAIL_STOP       = float(os.environ.get('TRAIL_STOP', '1.0'))         # lebar trailing = 1x dist
MIN_DIST_PCT     = float(os.environ.get('MIN_DIST_PCT', '0.002'))     # floor SL minimum 0.2%
SL_PCT           = float(os.environ.get('SL_PCT', '0.003'))           # jarak SL = 0.3% dari entry (wick),
                                                                        # MENGGANTIKAN jarak struktural candle

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

# FILTER berbasis indikator saat cross. DEFAULT AKTIF: ATR ratio >= 1.07 (candle penyebab
# cross harus minimal sebesar volatilitas normalnya) -- dari hasil analisis, ini indikator
# dgn spread win-rate paling konsisten & kuat. Override / matikan (isi 0) lewat Railway
# Variables. Tiap indikator sekarang punya MIN dan MAX (0 = sisi itu nonaktif) supaya bisa
# bikin filter "range" (misal cuma ambil yg di tengah, bukan cuma "makin besar makin bagus"),
# karena hasil riset menunjukkan pola bucket Rendah/Sedang/Tinggi TIDAK selalu monoton.
FILTER_MIN_ATR_RATIO   = float(os.environ.get('FILTER_MIN_ATR_RATIO', '1.07'))   # 0 = nonaktif
FILTER_MAX_ATR_RATIO   = float(os.environ.get('FILTER_MAX_ATR_RATIO', '0'))      # 0 = nonaktif
FILTER_MIN_VOL_RATIO   = float(os.environ.get('FILTER_MIN_VOL_RATIO', '0'))      # 0 = nonaktif
FILTER_MAX_VOL_RATIO   = float(os.environ.get('FILTER_MAX_VOL_RATIO', '0'))      # 0 = nonaktif
FILTER_MIN_EMA_GAP_PCT = float(os.environ.get('FILTER_MIN_EMA_GAP_PCT', '0'))    # 0 = nonaktif
FILTER_MAX_EMA_GAP_PCT = float(os.environ.get('FILTER_MAX_EMA_GAP_PCT', '0'))    # 0 = nonaktif
FILTER_MIN_DIST_PCT    = float(os.environ.get('FILTER_MIN_DIST_PCT', '0'))       # 0 = nonaktif
FILTER_MAX_DIST_PCT    = float(os.environ.get('FILTER_MAX_DIST_PCT', '0'))       # 0 = nonaktif

# RSI/MACD/SAR: sentinel nonaktif = string kosong (BUKAN 0), karena MACD histogram &
# jarak-ke-SAR bisa bernilai negatif secara wajar (0 tetap nilai valid utk keduanya).
def _env_float_opt(name):
    raw = os.environ.get(name, '').strip()
    return float(raw) if raw != '' else None

FILTER_MIN_RSI          = _env_float_opt('FILTER_MIN_RSI')            # None = nonaktif
FILTER_MAX_RSI          = _env_float_opt('FILTER_MAX_RSI')            # None = nonaktif
FILTER_MIN_MACD_HIST    = _env_float_opt('FILTER_MIN_MACD_HIST_PCT')  # None = nonaktif
FILTER_MAX_MACD_HIST    = _env_float_opt('FILTER_MAX_MACD_HIST_PCT')  # None = nonaktif
FILTER_MIN_SAR_DIST_PCT = _env_float_opt('FILTER_MIN_SAR_DIST_PCT')   # None = nonaktif
FILTER_MAX_SAR_DIST_PCT = _env_float_opt('FILTER_MAX_SAR_DIST_PCT')   # None = nonaktif


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
_rsi_gate_result = {}       # hasil analisis khusus RSI gate (per-nilai + bucket, terpisah Long/Short)
_session_day_result = {}    # hasil analisis sesi trading (WIB) & hari (Senin-Minggu)
_no_filter_result = None    # ringkasan simulasi PEMBANDING tanpa filter (None jika tidak ada filter aktif)


def _any_filter_active():
    return (
        FILTER_MIN_ATR_RATIO > 0 or FILTER_MAX_ATR_RATIO > 0 or
        FILTER_MIN_VOL_RATIO > 0 or FILTER_MAX_VOL_RATIO > 0 or
        FILTER_MIN_EMA_GAP_PCT > 0 or FILTER_MAX_EMA_GAP_PCT > 0 or
        FILTER_MIN_DIST_PCT > 0 or FILTER_MAX_DIST_PCT > 0 or
        FILTER_MIN_RSI is not None or FILTER_MAX_RSI is not None or
        FILTER_MIN_MACD_HIST is not None or FILTER_MAX_MACD_HIST is not None or
        FILTER_MIN_SAR_DIST_PCT is not None or FILTER_MAX_SAR_DIST_PCT is not None
    )


def _active_filter_summary():
    """List string ringkas filter yg sedang aktif, utk ditampilkan di dashboard."""
    parts = []
    if FILTER_MIN_ATR_RATIO > 0: parts.append(f"ATR ratio ≥ {FILTER_MIN_ATR_RATIO:.2f}x")
    if FILTER_MAX_ATR_RATIO > 0: parts.append(f"ATR ratio ≤ {FILTER_MAX_ATR_RATIO:.2f}x")
    if FILTER_MIN_VOL_RATIO > 0: parts.append(f"Vol ratio ≥ {FILTER_MIN_VOL_RATIO:.2f}x")
    if FILTER_MAX_VOL_RATIO > 0: parts.append(f"Vol ratio ≤ {FILTER_MAX_VOL_RATIO:.2f}x")
    if FILTER_MIN_EMA_GAP_PCT > 0: parts.append(f"EMA gap ≥ {FILTER_MIN_EMA_GAP_PCT:.3f}%")
    if FILTER_MAX_EMA_GAP_PCT > 0: parts.append(f"EMA gap ≤ {FILTER_MAX_EMA_GAP_PCT:.3f}%")
    if FILTER_MIN_DIST_PCT > 0: parts.append(f"Jarak SL ≥ {FILTER_MIN_DIST_PCT:.3f}%")
    if FILTER_MAX_DIST_PCT > 0: parts.append(f"Jarak SL ≤ {FILTER_MAX_DIST_PCT:.3f}%")
    if FILTER_MIN_RSI is not None: parts.append(f"RSI ≥ {FILTER_MIN_RSI:.1f}")
    if FILTER_MAX_RSI is not None: parts.append(f"RSI ≤ {FILTER_MAX_RSI:.1f}")
    if FILTER_MIN_MACD_HIST is not None: parts.append(f"MACD hist ≥ {FILTER_MIN_MACD_HIST:+.3f}%")
    if FILTER_MAX_MACD_HIST is not None: parts.append(f"MACD hist ≤ {FILTER_MAX_MACD_HIST:+.3f}%")
    if FILTER_MIN_SAR_DIST_PCT is not None: parts.append(f"Jarak SAR ≥ {FILTER_MIN_SAR_DIST_PCT:+.2f}%")
    if FILTER_MAX_SAR_DIST_PCT is not None: parts.append(f"Jarak SAR ≤ {FILTER_MAX_SAR_DIST_PCT:+.2f}%")
    return parts


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
    """Versi O(n) — logika & hasil identik dgn versi lama (O(n^2)), hanya cara
    cek 'broken' yang diubah dari re-scan mundur tiap event jadi single-pass maju.

    Trik: setiap level di stack disimpan bersama 'broken_at' = index candle pertama
    (j > c3 level itu) di mana harga menembus sl-nya. Nilai ini dihitung SEKALI saat
    level baru masuk stack (scan maju dari c3+1 sampai ketemu candle yg break, atau
    sampai akhir data), bukan diulang-ulang untuk tiap event baru yang muncul di
    depannya. Saat butuh cek "level ini masih hidup di candle X?", tinggal bandingkan
    broken_at > X -- O(1). Total kerja scan tetap O(n) sepanjang seluruh dataset
    (tiap tumpukan sequence candle dilewati sekali per level, bukan berulang).
    """
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

    # Precompute, per tipe, kapan tiap kemungkinan level "sl" pertama kali ditembus
    # kalau dipasang mulai dari index tertentu. Karena sl bisa beda2 per event, kita
    # tetap hitung broken_at per-level individual, tapi HANYA SEKALI per level (saat
    # level itu masuk stack) dgn scan maju yg berhenti begitu ketemu breach pertama --
    # bukan diulang utk tiap event baru yg overlap sepertinya versi lama.
    def _broken_at(ty, start_idx, sl_val):
        if ty == 'support':
            for j in range(start_idx, n):
                if c[j] < sl_val - 1e-12:
                    return j
        else:
            for j in range(start_idx, n):
                if c[j] > sl_val + 1e-12:
                    return j
        return n  # tidak pernah break dalam data

    stack = {'support': [], 'resistance': []}
    events = []
    for e in raw:
        ty = e['type']; cutoff = e['c1']
        # buang level yg sudah broken sebelum/at cutoff (O(1) per level krn broken_at
        # sudah dihitung duluan)
        stack[ty] = [ref for ref in stack[ty] if ref['broken_at'] > cutoff]
        prev = stack[ty][-1]['level'] if stack[ty] else None
        wick_extreme = e['sl']; S = e['level']
        if prev is None:
            e['valid'] = False
        elif ty == 'support':
            e['valid'] = (wick_extreme <= prev + 1e-12) and (prev <= S + 1e-12)
        else:
            e['valid'] = (wick_extreme >= prev - 1e-12) and (prev >= S - 1e-12)
        events.append(e)
        broken_at = _broken_at(ty, e['c3'] + 1, wick_extreme)
        stack[ty].append({'level': S, 'sl': wick_extreme, 'c3': e['c3'], 'broken_at': broken_at})
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
# INDIKATOR TAMBAHAN: RSI, MACD, PARABOLIC SAR
# (implementasi manual numpy/pandas, tanpa dependency TA-Lib)
# ============================================================

RSI_PERIOD       = int(os.environ.get('RSI_PERIOD', '14'))
MACD_FAST        = int(os.environ.get('MACD_FAST', '12'))
MACD_SLOW        = int(os.environ.get('MACD_SLOW', '26'))
MACD_SIGNAL      = int(os.environ.get('MACD_SIGNAL', '9'))
SAR_STEP         = float(os.environ.get('SAR_STEP', '0.02'))
SAR_MAX_STEP     = float(os.environ.get('SAR_MAX_STEP', '0.2'))

# ── GATE RSI TUNGGAL saat EMA cross (BUKAN filter statistik ambang batas -- ini kondisi
# STRUKTURAL yg dicek TEPAT di candle penyebab cross, sama level dgn syarat cross itu sendiri) ──
# RSI4 (periode pendek, reaktif thd harga). NONAKTIF secara default -- dipakai dulu utk
# eksplorasi via tabel Analisis Indikator (RSI per-nilai + bucket Rendah/Sedang/Tinggi),
# baru diaktifkan setelah tahu rentang RSI4 yg benar2 menguntungkan dari hasil analisis itu.
# Golden cross (bias Long)  valid HANYA jika RSI_GATE_MIN_LONG  <= RSI4 <= RSI_GATE_MAX_LONG
# Death cross  (bias Short) valid HANYA jika RSI_GATE_MIN_SHORT <= RSI4 <= RSI_GATE_MAX_SHORT
# Di luar rentang = diblokir (sinyal dilewati, tidak entry).
RSI_GATE_ENABLED    = os.environ.get('RSI_GATE_ENABLED', '0').strip() not in ('0', 'false', 'False', '')
RSI_GATE_PERIOD     = int(os.environ.get('RSI_GATE_PERIOD', '4'))
RSI_GATE_MIN_LONG   = float(os.environ.get('RSI_GATE_MIN_LONG', '0'))
RSI_GATE_MAX_LONG   = float(os.environ.get('RSI_GATE_MAX_LONG', '70'))
RSI_GATE_MIN_SHORT  = float(os.environ.get('RSI_GATE_MIN_SHORT', '40'))
RSI_GATE_MAX_SHORT  = float(os.environ.get('RSI_GATE_MAX_SHORT', '100'))

# ── Rentang FOKUS utk tabel Analisis Khusus RSI Gate (TERPISAH dari gate aktual di atas --
# ini cuma menyempitkan rentang yg dianalisis & dibagi 3 bucket, tidak mempengaruhi entry
# sama sekali walau RSI_GATE_ENABLED=1). Nilai diluar rentang fokus tetap dihitung ke
# n_total & per_value (spy total tetap akurat), tapi TIDAK masuk ke salah satu bucket.
# Bucket dibagi RATA OTOMATIS jadi 3 dari rentang fokus ini (span/3, integer boundary).
RSI_ANALYSIS_MIN_LONG  = float(os.environ.get('RSI_ANALYSIS_MIN_LONG', '41'))
RSI_ANALYSIS_MAX_LONG  = float(os.environ.get('RSI_ANALYSIS_MAX_LONG', '80'))
RSI_ANALYSIS_MIN_SHORT = float(os.environ.get('RSI_ANALYSIS_MIN_SHORT', '12'))
RSI_ANALYSIS_MAX_SHORT = float(os.environ.get('RSI_ANALYSIS_MAX_SHORT', '50'))


def _calc_rsi(C, period):
    """RSI standar (Wilder smoothing via EWM alpha=1/period).
    Rumus 100*avg_gain/(avg_gain+avg_loss) dipakai langsung (bukan 100-100/(1+RS))
    supaya kasus tepi avg_gain=avg_loss=0 (harga flat berturut-turut) otomatis
    jadi NaN alih-alih perlu di-patch manual -- identik dgn definisi RSI standar."""
    close = pd.Series(C)
    delta = close.diff(1)
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rsi = 100 * avg_gain / (avg_gain + avg_loss)
    return rsi.values


def _calc_macd(C, fast, slow, signal):
    """MACD line, signal line, histogram."""
    ema_fast = pd.Series(C).ewm(span=fast, adjust=False).mean().values
    ema_slow = pd.Series(C).ewm(span=slow, adjust=False).mean().values
    macd_line = ema_fast - ema_slow
    signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def _calc_psar(H, L, C, step=0.02, max_step=0.2):
    """Parabolic SAR (Wilder), urutan langkah mengikuti implementasi standar:
    1) proyeksi sar[i] dari sar[i-1], 2) tentukan reversal berdasarkan proyeksi itu,
    3) update EP/AF, 4) clamp sar[i] ke high/low candle SEBELUMNYA (i-1) saja,
    5) kalau reversal, timpa sar[i] = ep lama & reset AF/EP.
    Return (sar, is_falling_bool_array) -- falling=True berarti SAR di ATAS harga (downtrend)."""
    n = len(C)
    sar = np.zeros(n)
    falling = np.zeros(n, dtype=bool)
    if n == 0:
        return sar, falling
    falling[0] = H[0] < H[0]  # placeholder, ditentukan dari 2 candle pertama di bawah
    # tentukan arah awal dari candle 0->1 (naik/turun net) -- konsisten dgn referensi:
    is_falling = L[0] > L[1] if n > 1 else False if n > 0 else False
    # (fallback sederhana bila n==1 tak relevan krn warmup jauh lebih besar dari 1)
    ep = L[0] if is_falling else H[0]
    af = step
    sar[0] = C[0] if len(C) else (H[0] if is_falling else L[0])
    falling[0] = is_falling
    for i in range(1, n):
        proj = sar[i - 1] + af * (ep - sar[i - 1])
        if is_falling:
            reverse = H[i] > proj
            if L[i] < ep:
                ep = L[i]
                af = min(af + step, max_step)
            proj = max(H[i - 1], proj)
        else:
            reverse = L[i] < proj
            if H[i] > ep:
                ep = H[i]
                af = min(af + step, max_step)
            proj = min(L[i - 1], proj)
        if reverse:
            proj = ep
            af = step
            is_falling = not is_falling
            ep = L[i] if is_falling else H[i]
        sar[i] = proj
        falling[i] = is_falling
    return sar, ~falling  # kembalikan sbg "is_uptrend" (kebalikan dari falling) spy sesuai nama lama


# ============================================================
# INDIKATOR SAAT CROSS (utk analisis pola menang/kalah)
# ============================================================

def _capture_indicators(c, i):
    """Ambil snapshot indikator persis di candle i (candle penyebab EMA cross)."""
    close_i = c['C'][i]
    vol_ma  = c['vol_ma'][i]
    atr_i   = c['atr'][i]
    rng     = c['H'][i] - c['L'][i]
    rsi_i   = c['rsi'][i]
    macd_i  = c['macd_hist'][i]
    sar_i   = c['sar'][i]
    sar_up  = c['sar_up'][i]
    rsi_gate_i = c['rsi_gate'][i]
    return {
        'vol_ratio': (c['V'][i] / vol_ma) if vol_ma > 0 else None,          # volume vs rata2 20 candle
        'atr_ratio': (rng / atr_i) if atr_i > 0 else None,                  # besar candle vs volatilitas normal
        'ema_gap_pct': (abs(c['ema_fast'][i] - c['ema_slow'][i]) / close_i * 100) if close_i else None,
        'trend_pct': ((close_i - c['ema_trend'][i]) / c['ema_trend'][i] * 100) if c['ema_trend'][i] else None,
        'dist_pct': None,   # diisi setelah dist final diketahui (lihat di bawah)
        'rsi': (float(rsi_i) if not np.isnan(rsi_i) else None),             # RSI14 saat cross
        'macd_hist_pct': ((macd_i / close_i * 100) if close_i else None),   # histogram MACD, dinormalisasi ke % harga
        'sar_dist_pct': ((close_i - sar_i) / close_i * 100) if close_i else None,  # jarak close ke PSAR (%), + = di atas SAR (uptrend PSAR)
        f'rsi{RSI_GATE_PERIOD}': (float(rsi_gate_i) if not np.isnan(rsi_gate_i) else None),  # RSI gate (periode pendek)
    }


def _passes_rsi_gate(c, i, direction):
    """Gate RSI TUNGGAL saat EMA cross (bukan filter statistik -- kondisi STRUKTURAL).
    Rentang penuh [MIN, MAX] terpisah utk Long & Short -- diluar rentang = diblokir.
    direction: 'Long' butuh RSI_GATE_MIN_LONG <= RSI <= RSI_GATE_MAX_LONG; 'Short' butuh
    RSI_GATE_MIN_SHORT <= RSI <= RSI_GATE_MAX_SHORT, persis di candle cross yg sama.
    True kalau gate nonaktif ATAU nilai RSI belum tersedia (masih warmup) -- fail-open
    spy tidak diam-diam menolak semua trade di awal data krn NaN."""
    if not RSI_GATE_ENABLED:
        return True
    v = c['rsi_gate'][i]
    if np.isnan(v):
        return True
    if direction == 'Long':
        return RSI_GATE_MIN_LONG <= v <= RSI_GATE_MAX_LONG
    else:
        return RSI_GATE_MIN_SHORT <= v <= RSI_GATE_MAX_SHORT


def prepare_coin(symbol, df):
    """Precompute semua yang dibutuhkan simulasi + indikator (utk analisis win/loss) utk 1 koin.
    None kalau data kurang."""
    n = len(df)
    warmup = max(EMA_SLOW, EMA_TREND, VOL_MA_PERIOD, ATR_PERIOD, RSI_PERIOD, MACD_SLOW,
                 RSI_GATE_PERIOD) + 10
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
    rsi = _calc_rsi(C, RSI_PERIOD)
    rsi_gate = _calc_rsi(C, RSI_GATE_PERIOD)   # RSI4 default -- utk gate cross, TERPISAH dari RSI_PERIOD analisis
    _, _, macd_hist = _calc_macd(C, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    sar, sar_up = _calc_psar(H, L, C, SAR_STEP, SAR_MAX_STEP)
    events = find_sr_events(df)
    events_by_c3 = {}
    for e in events:
        events_by_c3.setdefault(e['c3'], []).append(e)
    return {
        'symbol': symbol, 'O': O, 'H': H, 'L': L, 'C': C, 'V': V, 'TS': TS,
        'ema_fast': ema_fast, 'ema_slow': ema_slow, 'ema_trend': ema_trend,
        'rsi_gate': rsi_gate,
        'vol_ma': vol_ma, 'atr': atr, 'rsi': rsi, 'macd_hist': macd_hist,
        'sar': sar, 'sar_up': sar_up, 'events_by_c3': events_by_c3,
        'n': n, 'warmup': warmup,
        'ts_to_idx': {int(TS[i]): i for i in range(n)},
    }


# ============================================================
# SIMULASI GABUNGAN — SEMUA KOIN BERBARENGAN, 1 BALANCE (COMPOUNDING),
# 1 POOL MAX_CONCURRENT (persis seperti bot live: satu akun, slot terbatas
# dipakai bersama oleh semua koin, bukan simulasi per-koin terisolasi)
# ============================================================

def run_combined_backtest(coins: dict, filters_enabled: bool = True) -> dict:
    """coins: {symbol: prepared_dict dari prepare_coin()}
    filters_enabled=False -> semua FILTER_* diabaikan (dipakai utk simulasi pembanding
    'tanpa filter' pada bagian Dampak Filter Aktif)."""
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
    blocked_by_rsi_gate = 0  # counter: dilewati krn gate RSI12 vs RSI24 tidak searah dgn cross

    def _akey(symbol, direction):
        return f"{symbol}|{direction}" if ALLOW_HEDGE else symbol

    def _slots_used():
        return len(active_positions) + len(pending)

    def _current_margin_used():
        """Total margin yg sedang dipakai SEMUA posisi terbuka (notional/leverage) --
        persis seperti akun Bybit riil, ini yg membatasi berapa banyak posisi bisa
        dibuka bersamaan, BUKAN sekadar persentase risiko."""
        return sum((p['entry'] * p['qty']) / LEVERAGE for p in active_positions.values())

    def _passes_filters(ind, enabled=True):
        if not enabled:
            return True
        v = ind.get('atr_ratio')
        if FILTER_MIN_ATR_RATIO > 0 and (v is None or v < FILTER_MIN_ATR_RATIO):
            return False
        if FILTER_MAX_ATR_RATIO > 0 and (v is None or v > FILTER_MAX_ATR_RATIO):
            return False
        v = ind.get('vol_ratio')
        if FILTER_MIN_VOL_RATIO > 0 and (v is None or v < FILTER_MIN_VOL_RATIO):
            return False
        if FILTER_MAX_VOL_RATIO > 0 and (v is None or v > FILTER_MAX_VOL_RATIO):
            return False
        v = ind.get('ema_gap_pct')
        if FILTER_MIN_EMA_GAP_PCT > 0 and (v is None or v < FILTER_MIN_EMA_GAP_PCT):
            return False
        if FILTER_MAX_EMA_GAP_PCT > 0 and (v is None or v > FILTER_MAX_EMA_GAP_PCT):
            return False
        v = ind.get('dist_pct_est')
        if FILTER_MIN_DIST_PCT > 0 and (v is None or v < FILTER_MIN_DIST_PCT):
            return False
        if FILTER_MAX_DIST_PCT > 0 and (v is None or v > FILTER_MAX_DIST_PCT):
            return False
        v = ind.get('rsi')
        if FILTER_MIN_RSI is not None and (v is None or v < FILTER_MIN_RSI):
            return False
        if FILTER_MAX_RSI is not None and (v is None or v > FILTER_MAX_RSI):
            return False
        v = ind.get('macd_hist_pct')
        if FILTER_MIN_MACD_HIST is not None and (v is None or v < FILTER_MIN_MACD_HIST):
            return False
        if FILTER_MAX_MACD_HIST is not None and (v is None or v > FILTER_MAX_MACD_HIST):
            return False
        v = ind.get('sar_dist_pct')
        if FILTER_MIN_SAR_DIST_PCT is not None and (v is None or v < FILTER_MIN_SAR_DIST_PCT):
            return False
        if FILTER_MAX_SAR_DIST_PCT is not None and (v is None or v > FILTER_MAX_SAR_DIST_PCT):
            return False
        return True

    def close_trade(symbol, direction, exit_price, reason, exit_ts):
        nonlocal balance
        key = _akey(symbol, direction)
        pos = active_positions[key]
        entry, dist, qty = pos['entry'], pos['dist'], pos['qty']
        pnl_gross = (exit_price - entry) * qty if direction == 'Long' else (entry - exit_price) * qty
        fee = entry * qty * FEE_ENTRY_PCT + exit_price * qty * FEE_EXIT_PCT
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

            # ── 1) FLIP PROTECTION murni EMA cross (TANPA syarat RSI) — cross berlawanan
            #    langsung membatalkan limit pending / menutup posisi filled, apapun P&L-nya.
            #    Bias tetap hidup, tunggu cross searah lagi utk re-entry. ──
            if death_cross:
                pending.pop(key_long, None)
                if key_long in active_positions:
                    close_trade(symbol, 'Long', O[i+1], 'FLIP', int(TS[i+1]))
            if golden_cross:
                pending.pop(key_short, None)
                if key_short in active_positions:
                    close_trade(symbol, 'Short', O[i+1], 'FLIP', int(TS[i+1]))

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

            # ── 5) cross SEARAH -> pasang/ganti limit di wick, TUNDUK ke RSI GATE, MAX_CONCURRENT & FILTER ──
            # Tangkap indikator PERSIS di candle cross ini -> dipakai jg utk filter & analisis win/loss.
            if death_cross and key_short in armed and key_short not in active_positions:
                wick = H[i]; old_dist = wick * SL_PCT   # SL = SL_PCT dari entry (wick), bukan jarak struktural candle
                if old_dist > 0:
                    if not _passes_rsi_gate(c, i, 'Short'):
                        blocked_by_rsi_gate += 1
                    else:
                        ind = _capture_indicators(c, i)
                        ind['dist_pct_est'] = (old_dist / wick * 100) if wick else None
                        if not _passes_filters(ind, filters_enabled):
                            blocked_by_filter += 1
                        elif key_short not in pending and _slots_used() >= MAX_CONCURRENT:
                            blocked_by_slot += 1
                        else:
                            pending[key_short] = {
                                'entry': wick, 'sl': wick + old_dist, 'dist': old_dist, 'ind': ind,
                            }

            if golden_cross and key_long in armed and key_long not in active_positions:
                wick = L[i]; old_dist = wick * SL_PCT   # SL = SL_PCT dari entry (wick), bukan jarak struktural candle
                if old_dist > 0:
                    if not _passes_rsi_gate(c, i, 'Long'):
                        blocked_by_rsi_gate += 1
                    else:
                        ind = _capture_indicators(c, i)
                        ind['dist_pct_est'] = (old_dist / wick * 100) if wick else None
                        if not _passes_filters(ind, filters_enabled):
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
        'blocked_by_filter': blocked_by_filter, 'blocked_by_rsi_gate': blocked_by_rsi_gate,
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
    'vol_ratio':      {'label': 'Volume saat cross (vs rata² 20 candle)', 'fmt': '{:.2f}x'},
    'atr_ratio':      {'label': 'Ukuran candle cross (vs ATR14)',         'fmt': '{:.2f}x'},
    'ema_gap_pct':    {'label': 'Jarak EMA4-EMA10 saat cross',            'fmt': '{:.3f}%'},
    'trend_pct':      {'label': 'Posisi close vs EMA50 (tren besar)',     'fmt': '{:+.2f}%'},
    'dist_pct':       {'label': 'Jarak SL dari entry',                    'fmt': '{:.3f}%'},
    'rsi':            {'label': f'RSI{RSI_PERIOD} saat cross',            'fmt': '{:.1f}'},
    'macd_hist_pct':  {'label': f'MACD histogram ({MACD_FAST}/{MACD_SLOW}/{MACD_SIGNAL}, %harga)', 'fmt': '{:+.3f}%'},
    'sar_dist_pct':   {'label': 'Jarak close ke Parabolic SAR (%)',       'fmt': '{:+.2f}%'},
}

def indicator_analysis(trades):
    """Utk tiap indikator: avg saat WIN vs LOSS, WR & win/loss count utk SEMUA trade (agregat),
    + win rate & win/loss count per bucket KUMULATIF Rendah/Sedang/Tinggi.

    PENTING - definisi bucket (KUMULATIF, bukan partisi/rentang terpisah):
      Rendah  = SEMUA trade dgn nilai indikator <= t1  (spt filter "maks t1")
      Sedang  = SEMUA trade dgn nilai indikator <= t2  (spt filter "maks t2", t2 > t1
                jadi Sedang selalu superset dari Rendah, BUKAN cuma yg di antara t1-t2)
      Tinggi  = SEMUA trade, tanpa batas atas (= sama dgn "Semua Trade")
    Ketiganya dievaluasi terhadap TOTAL trade yang sama (n identik = jumlah trade
    keseluruhan yg punya nilai indikator ini), bukan dipecah jadi 3 kelompok terpisah.
    Tujuannya: lihat langsung "kalau saya pasang threshold di t1 (atau t2), berapa
    trade yang lolos dan berapa yang MENANG secara absolut" -- bukan proporsi bucket."""
    out = {}
    for key, info in INDICATOR_INFO.items():
        pairs = [(t[key], t['r_mult'] > 0) for t in trades if t.get(key) is not None]
        if len(pairs) < 6:
            continue
        win_vals  = [v for v, w in pairs if w]
        loss_vals = [v for v, w in pairs if not w]
        vmin = min(v for v, _ in pairs)
        vmax = max(v for v, _ in pairs)
        span = vmax - vmin
        if span <= 0:
            t1 = t2 = vmin
        else:
            t1 = vmin + span / 3
            t2 = vmin + 2 * span / 3

        def _cum_bucket(threshold, no_limit=False):
            ws = [w for v, w in pairs if no_limit or v <= threshold]
            return {
                'wr': (sum(ws) / len(ws) * 100) if ws else None,
                'n': len(ws),
                'n_win': sum(1 for w in ws if w),
                'n_loss': sum(1 for w in ws if not w),
            }

        buckets = {
            'Rendah': _cum_bucket(t1),
            'Sedang': _cum_bucket(t2),
            'Tinggi': _cum_bucket(None, no_limit=True),
        }
        n_win_all = len(win_vals)
        n_loss_all = len(loss_vals)
        out[key] = {
            'label': info['label'], 'fmt': info['fmt'],
            'avg_win': (sum(win_vals) / len(win_vals)) if win_vals else None,
            'avg_loss': (sum(loss_vals) / len(loss_vals)) if loss_vals else None,
            'n': len(pairs), 't1': t1, 't2': t2,
            'n_win_all': n_win_all, 'n_loss_all': n_loss_all,
            'wr_all': (n_win_all / len(pairs) * 100) if pairs else None,
            'buckets': buckets,
        }
    return out


def rsi_gate_analysis(trades):
    """Analisis KHUSUS utk RSI gate (RSI{RSI_GATE_PERIOD}), TERPISAH Long vs Short:
    - per_value: utk tiap nilai RSI BULAT (0-100), jumlah win/loss/WR -- supaya kelihatan
      angka RSI spesifik mana yang paling sering menang, bukan cuma rentang besar.
    - buckets: 3 kelompok dibagi RATA OTOMATIS dari rentang FOKUS
      (RSI_ANALYSIS_MIN_LONG..MAX_LONG utk Long, RSI_ANALYSIS_MIN_SHORT..MAX_SHORT utk
      Short) -- span/3, batas bulat. Trade DILUAR rentang fokus tetap dihitung ke n_total
      & per_value, tapi tidak masuk bucket manapun (supaya bucket cuma merepresentasikan
      rentang yg sedang difokuskan, bukan ikut numpuk di tepi).
    - best_value: nilai RSI bulat dgn jumlah MENANG absolut terbanyak per arah (bukan cuma
      WR% tertinggi -- supaya tidak kejebak angka dgn n kecil tapi WR 100%)."""
    rsi_key = f'rsi{RSI_GATE_PERIOD}'
    focus_range = {
        'Long':  (RSI_ANALYSIS_MIN_LONG, RSI_ANALYSIS_MAX_LONG),
        'Short': (RSI_ANALYSIS_MIN_SHORT, RSI_ANALYSIS_MAX_SHORT),
    }
    out = {}
    for direction in ('Long', 'Short'):
        pairs = [(t[rsi_key], t['r_mult'] > 0) for t in trades
                 if t.get(rsi_key) is not None and t.get('direction') == direction]

        fmin, fmax = focus_range[direction]
        fmin_i, fmax_i = int(round(fmin)), int(round(fmax))
        span = max(fmax_i - fmin_i, 1)
        b1 = fmin_i + span // 3               # batas Rendah/Sedang
        b2 = fmin_i + (2 * span) // 3          # batas Sedang/Tinggi
        bucket_names = {
            'Rendah': f'Rendah ({fmin_i}-{b1})',
            'Sedang': f'Sedang ({b1+1}-{b2})',
            'Tinggi': f'Tinggi ({b2+1}-{fmax_i})',
        }
        buckets = {name: {'n': 0, 'n_win': 0, 'n_loss': 0} for name in bucket_names.values()}

        per_value = {}   # nilai RSI bulat (0-100) -> {n, n_win, n_loss, wr}
        for v, w in pairs:
            iv = int(round(v))
            iv = max(0, min(100, iv))   # clamp jaga2 kalau RSI numerik meleset dikit dari [0,100]
            slot = per_value.setdefault(iv, {'n': 0, 'n_win': 0, 'n_loss': 0})
            slot['n'] += 1
            slot['n_win'] += 1 if w else 0
            slot['n_loss'] += 0 if w else 1
            if fmin_i <= iv <= fmax_i:   # hanya masuk bucket kalau di dalam rentang fokus
                bname = bucket_names['Rendah'] if iv <= b1 else (bucket_names['Sedang'] if iv <= b2 else bucket_names['Tinggi'])
                buckets[bname]['n'] += 1
                buckets[bname]['n_win'] += 1 if w else 0
                buckets[bname]['n_loss'] += 0 if w else 1
        for slot in per_value.values():
            slot['wr'] = (slot['n_win'] / slot['n'] * 100) if slot['n'] else None
        for b in buckets.values():
            b['wr'] = (b['n_win'] / b['n'] * 100) if b['n'] else None
        best_value = None
        if per_value:
            best_value = max(per_value.items(), key=lambda kv: kv[1]['n_win'])[0]
        n_in_focus = sum(b['n'] for b in buckets.values())
        out[direction] = {
            'n_total': len(pairs),
            'n_in_focus': n_in_focus,     # jumlah trade yg masuk rentang fokus (<= n_total)
            'focus_range': (fmin_i, fmax_i),
            'per_value': per_value,      # {0: {...}, 1: {...}, ..., 100: {...}}
            'buckets': buckets,
            'best_value': best_value,    # nilai RSI bulat dgn n_win TERBANYAK (bukan cuma WR tertinggi)
        }
    return out


# Definisi sesi trading (WIB / UTC+7). Batas jam INKLUSIF di kedua ujung sesuai spesifikasi:
# Asia 07:00-13:59, London 14:00-18:59, New York 19:00-23:59, Tengah Malam 00:00-04:59,
# Sydney 05:00-06:59. Urutan list menentukan urutan tampil di dashboard.
_SESSION_DEFS = [
    ('Sydney',       5,  6),
    ('Asia',         7,  13),
    ('London',       14, 18),
    ('New York',     19, 23),
    ('Tengah Malam', 0,  4),
]
_WIB = timezone(timedelta(hours=7))
_DAY_NAMES_ID = ['Senin', 'Selasa', 'Rabu', 'Kamis', "Jumat", 'Sabtu', 'Minggu']


def _session_for_hour(hour_wib):
    for name, start_h, end_h in _SESSION_DEFS:
        if start_h <= hour_wib <= end_h:
            return name
    return None   # tidak akan pernah kejadian krn 5 sesi di atas menutupi 24 jam penuh


def session_day_analysis(trades):
    """Analisis SESI TRADING (WIB, berdasar jam candle SAAT FILLED / entry_ts -- bukan saat
    sinyal cross muncul, sesuai definisi: cross jam 13:00 tapi filled jam 14:00 -> masuk
    sesi London) dan HARI (Senin-Minggu, dari entry_ts yg sama). Tiap kelompok: total trade,
    menang, kalah, WR%."""
    sessions = {name: {'n': 0, 'n_win': 0, 'n_loss': 0} for name, _, _ in _SESSION_DEFS}
    days = {name: {'n': 0, 'n_win': 0, 'n_loss': 0} for name in _DAY_NAMES_ID}
    for t in trades:
        ts = t.get('entry_ts')
        if ts is None:
            continue
        dt_wib = datetime.fromtimestamp(ts / 1000, tz=_WIB)
        win = t['r_mult'] > 0
        sname = _session_for_hour(dt_wib.hour)
        if sname is not None:
            s = sessions[sname]
            s['n'] += 1
            s['n_win'] += 1 if win else 0
            s['n_loss'] += 0 if win else 1
        dname = _DAY_NAMES_ID[dt_wib.weekday()]   # Monday=0 -> 'Senin'
        d = days[dname]
        d['n'] += 1
        d['n_win'] += 1 if win else 0
        d['n_loss'] += 0 if win else 1
    for s in sessions.values():
        s['wr'] = (s['n_win'] / s['n'] * 100) if s['n'] else None
    for d in days.values():
        d['wr'] = (d['n_win'] / d['n'] * 100) if d['n'] else None
    return {'sessions': sessions, 'days': days}


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
    rsi_gate_res = rsi_gate_analysis(result['trades'])
    session_day_res = session_day_analysis(result['trades'])

    no_filter_summary = None
    if _any_filter_active():
        _log_msg("🔁 Filter aktif terdeteksi — menjalankan simulasi PEMBANDING tanpa filter "
                  "utk mengukur dampaknya (n trade, WR, PnL)...")
        result_nf = run_combined_backtest(coins, filters_enabled=False)
        no_filter_summary = {
            'n_trades': result_nf['n_trades'], 'wr': result_nf['wr'],
            'total_pnl': result_nf['total_pnl'], 'roi': result_nf['roi'],
            'total_r': result_nf['total_r'], 'avg_r': result_nf['avg_r'],
            'final_balance': result_nf['final_balance'],
        }
        _log_msg(f"   Tanpa filter: {result_nf['n_trades']} trade | WR {result_nf['wr']:.1f}% | "
                  f"PnL ${result_nf['total_pnl']:+.2f} | Total R {result_nf['total_r']:+.2f}")

    with _lock:
        _combined_result.update(result)
        _all_trades.extend(result['trades'])
        _results.extend(per_symbol_breakdown(result['trades']))
        _indicator_result.update(ind_analysis)
        _rsi_gate_result.update(rsi_gate_res)
        _session_day_result.update(session_day_res)
        global _no_filter_result
        _no_filter_result = no_filter_summary
        _phase = 'done'

    _log_msg(f"🏁 Selesai! {result['n_trades']} trade | WR {result['wr']:.1f}% | "
              f"PnL ${result['total_pnl']:+.2f} | ROI {result['roi']:+.1f}% | "
              f"Balance akhir ${result['final_balance']:.2f} | "
              f"blocked: {result['blocked_by_slot']} (slot), {result['blocked_by_margin']} (margin), "
              f"{result['blocked_by_filter']} (filter indikator), "
              f"{result['blocked_by_rsi_gate']} (RSI{RSI_GATE_PERIOD} gate)")


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
        rsi_gate_cp = dict(_rsi_gate_result)
        session_day_cp = dict(_session_day_result)
        nf_cp = dict(_no_filter_result) if _no_filter_result else None

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
    blocked_rsi_gate = cr.get('blocked_by_rsi_gate', 0)

    summary_html = f'''
    <h2>Ringkasan Gabungan — 1 balance, 1 pool slot (COMPOUNDING) ({n_done}/{n_total} coin dimuat)</h2>
    <table>
      <tr><th>Total Trade</th><th>Win</th><th>Loss</th><th>WR%</th><th>Total PnL</th>
          <th>ROI%</th><th>Balance Akhir</th><th>Avg R/trade</th><th>Profit Factor</th>
          <th>Blokir: Slot</th><th>Blokir: Margin</th><th>Blokir: Filter</th><th>Blokir: RSI Gate</th></tr>
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
        <td class="y">{blocked_rsi_gate}</td>
      </tr>
    </table>
    <p style="font-size:12px;color:#8b949e">Leverage: <b>{LEVERAGE:.0f}x</b> | Margin usage cap: maksimal
    <b>{MARGIN_USAGE_CAP*100:.0f}%</b> dari balance boleh dipakai sbg margin bersamaan (dari SEMUA posisi
    terbuka -- persis constraint margin Bybit asli, bukan cuma persentase risiko). Fee: entry
    <b>{FEE_ENTRY_PCT*100:.3f}%</b>, exit <b>{FEE_EXIT_PCT*100:.3f}%</b> ({FEE_EXIT_PCT/FEE_ENTRY_PCT:.0f}x fee entry) —
    atur via env var <code>FEE_ENTRY_PCT</code>/<code>FEE_EXIT_PCT</code>. Filter aktif: {
        ', '.join(_active_filter_summary()) or 'tidak ada (semua nonaktif)'}
    <br>Gate RSI{RSI_GATE_PERIOD} saat cross: <b>{'AKTIF' if RSI_GATE_ENABLED else 'NONAKTIF'}</b>
    (Golden cross/Long butuh RSI{RSI_GATE_PERIOD} di rentang [{RSI_GATE_MIN_LONG:.0f}, {RSI_GATE_MAX_LONG:.0f}],
    Death cross/Short butuh RSI{RSI_GATE_PERIOD} di rentang [{RSI_GATE_MIN_SHORT:.0f}, {RSI_GATE_MAX_SHORT:.0f}]
    — diluar rentang diblokir. Atur via env var <code>RSI_GATE_ENABLED</code>, <code>RSI_GATE_PERIOD</code>,
    <code>RSI_GATE_MIN_LONG</code>/<code>RSI_GATE_MAX_LONG</code>, <code>RSI_GATE_MIN_SHORT</code>/<code>RSI_GATE_MAX_SHORT</code>)</p>
    '''

    # ── Dampak Filter Aktif: bandingkan DENGAN filter (hasil di atas) vs simulasi
    #    PEMBANDING tanpa filter, dijalankan sekali di awal saat filter terdeteksi aktif ──
    filter_impact_html = ''
    if nf_cp is not None:
        active_filters = _active_filter_summary()
        filt_chips = ''.join(f'<span class="chip" style="background:#1f6feb33;color:#58a6ff;margin:2px">{f}</span> '
                              for f in active_filters)
        n_delta = total_trades - nf_cp['n_trades']
        n_delta_pct = (n_delta / nf_cp['n_trades'] * 100) if nf_cp['n_trades'] else 0
        wr_delta = wr_overall - nf_cp['wr']
        pnl_delta = total_pnl - nf_cp['total_pnl']
        totalr_cur = cr.get('total_r', 0)
        totalr_delta = totalr_cur - nf_cp['total_r']

        def _dc(v):  # kelas warna utk delta (hijau kalau naik, merah kalau turun)
            return 'g' if v > 0 else ('r' if v < 0 else 'y')

        filter_impact_html = f'''
        <h2>⚖️ Dampak Filter Aktif — Dengan Filter vs Tanpa Filter</h2>
        <p class="note">Filter yang sedang aktif: {filt_chips or '(tidak ada)'}</p>
        <div class="tbl-wrap">
        <table>
          <tr><th></th><th>N Trade</th><th>WR%</th><th>Total PnL</th><th>Total R</th><th>Balance Akhir</th></tr>
          <tr>
            <td>🔒 DENGAN filter (hasil aktif)</td>
            <td>{total_trades}</td>
            <td class="{'g' if wr_overall>=40 else ('y' if wr_overall>=25 else 'r')}">{wr_overall:.1f}%</td>
            <td class="{'g' if total_pnl>=0 else 'r'}">${total_pnl:+,.2f}</td>
            <td class="{'g' if totalr_cur>=0 else 'r'}">{totalr_cur:+.2f}</td>
            <td>${final_bal:,.2f}</td>
          </tr>
          <tr>
            <td>🔓 TANPA filter (simulasi pembanding)</td>
            <td>{nf_cp['n_trades']}</td>
            <td>{nf_cp['wr']:.1f}%</td>
            <td>${nf_cp['total_pnl']:+,.2f}</td>
            <td>{nf_cp['total_r']:+.2f}</td>
            <td>${nf_cp['final_balance']:,.2f}</td>
          </tr>
          <tr style="border-top:2px solid #30363d">
            <td><b>Selisih (Filter − Tanpa Filter)</b></td>
            <td class="{_dc(n_delta)}">{n_delta:+d} trade ({n_delta_pct:+.1f}%)</td>
            <td class="{_dc(wr_delta)}">{wr_delta:+.1f}pp</td>
            <td class="{_dc(pnl_delta)}">${pnl_delta:+,.2f}</td>
            <td class="{_dc(totalr_delta)}">{totalr_delta:+.2f}</td>
            <td class="{_dc(final_bal - nf_cp['final_balance'])}">${final_bal - nf_cp['final_balance']:+,.2f}</td>
          </tr>
        </table>
        </div>
        <div class="note">
          💡 Baris "Selisih" menjawab: apakah filter ini <b>sepadan</b>? Kalau N Trade berkurang drastis
          tapi WR/Total R/PnL cuma naik tipis (atau malah turun), filter itu kemungkinan MEMBUANG
          peluang tanpa manfaat yang cukup. Filter yang baik: N Trade berkurang wajar, tapi Total R
          & PnL naik lebih dari proporsional (karena membuang trade-trade yang kalah lebih banyak
          daripada yang menang).
          <br>⚙️ Atur ambang batas via Railway Variables: <code>FILTER_MIN_ATR_RATIO</code>,
          <code>FILTER_MAX_ATR_RATIO</code>, <code>FILTER_MIN_VOL_RATIO</code>, <code>FILTER_MAX_VOL_RATIO</code>,
          <code>FILTER_MIN_EMA_GAP_PCT</code>, <code>FILTER_MAX_EMA_GAP_PCT</code>,
          <code>FILTER_MIN_DIST_PCT</code>, <code>FILTER_MAX_DIST_PCT</code>,
          <code>FILTER_MIN_RSI</code>, <code>FILTER_MAX_RSI</code>,
          <code>FILTER_MIN_MACD_HIST_PCT</code>, <code>FILTER_MAX_MACD_HIST_PCT</code>,
          <code>FILTER_MIN_SAR_DIST_PCT</code>, <code>FILTER_MAX_SAR_DIST_PCT</code>
          (isi salah satu sisi saja utk filter satu-arah, isi MIN+MAX utk filter "range"/tengah,
          kosongkan/hapus utk nonaktifkan sisi itu). Lihat nilai ambang batas per-indikator
          di tabel "Analisis Indikator" di bawah sebagai referensi angka.
          <br>Simulasi pembanding ini dijalankan otomatis SEKALI di awal saat filter terdeteksi aktif —
          jalankan ulang backtest (restart service) setelah mengubah env var utk lihat dampak barunya.
        </div>
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
                return f'<td class="y">-</td><td style="color:#8b949e">0 (0M/0K)</td>'
            cls = 'g' if info['wr'] >= 45 else ('y' if info['wr'] >= 30 else 'r')
            return (f'<td class="{cls}">{info["wr"]:.1f}%</td>'
                    f'<td style="color:#8b949e">{info["n"]} '
                    f'(<span class="g">{info["n_win"]}M</span>/<span class="r">{info["n_loss"]}K</span>)</td>')

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
      <tr><th rowspan="2">Indikator (saat candle cross)</th><th rowspan="2">Avg saat WIN</th><th rowspan="2">Avg saat LOSS</th>
          <th colspan="2">Rendah (≤ t1)</th><th colspan="2">Sedang (≤ t2)</th><th colspan="2">Tinggi (semua, tanpa batas)</th>
          <th rowspan="2">t1 / t2</th></tr>
      <tr><th>WR%</th><th>N (Menang/Kalah)</th>
          <th>WR%</th><th>N (Menang/Kalah)</th><th>WR%</th><th>N (Menang/Kalah)</th></tr>
      {ind_rows or '<tr><td colspan="10" class="y">Belum ada data (minimal 6 trade per indikator).</td></tr>'}
    </table>
    ''' if ind_cp else '<p class="note">Belum ada data indikator.</p>'

    # ── tabel khusus RSI GATE: per-nilai RSI bulat (diurutkan by n_win terbanyak) + bucket tetap ──
    def _rsi_gate_side_html(direction):
        d = rsi_gate_cp.get(direction)
        if not d or d['n_total'] == 0:
            return f'<p class="note">Belum ada trade {direction} dgn data RSI{RSI_GATE_PERIOD}.</p>'
        # per-nilai: urutkan by jumlah MENANG absolut terbanyak dulu, lalu tampilkan top 15
        rows_sorted = sorted(d['per_value'].items(), key=lambda kv: kv[1]['n_win'], reverse=True)
        per_val_rows = ''
        for rsi_val, info in rows_sorted[:15]:
            wr = info['wr']
            cls = 'g' if (wr or 0) >= 50 else ('y' if (wr or 0) >= 30 else 'r')
            star = ' ⭐' if rsi_val == d['best_value'] else ''
            in_focus = d['focus_range'][0] <= rsi_val <= d['focus_range'][1]
            dim = '' if in_focus else ' style="color:#6e7681"'   # nilai diluar fokus ditampilkan redup
            per_val_rows += (f'<tr{dim}><td>{rsi_val}{star}</td><td>{info["n"]}</td>'
                              f'<td class="g">{info["n_win"]}</td><td class="r">{info["n_loss"]}</td>'
                              f'<td class="{cls}">{wr:.1f}%</td></tr>\n')
        # bucket names sekarang DINAMIS (mengikuti rentang fokus) -- iterasi dict.items()
        # langsung supaya urutan insert (Rendah, Sedang, Tinggi) terjaga
        bucket_rows = ''
        for bname, bi in d['buckets'].items():
            wr = bi['wr']
            cls = 'y' if wr is None else ('g' if wr >= 50 else ('y' if wr >= 30 else 'r'))
            wr_s = f'{wr:.1f}%' if wr is not None else '-'
            bucket_rows += (f'<tr><td>{bname}</td><td>{bi["n"]}</td>'
                             f'<td class="g">{bi["n_win"]}</td><td class="r">{bi["n_loss"]}</td>'
                             f'<td class="{cls}">{wr_s}</td></tr>\n')
        best_s = f"RSI{RSI_GATE_PERIOD} = {d['best_value']}" if d['best_value'] is not None else '-'
        fmin, fmax = d['focus_range']
        return f'''
        <p style="font-size:13px;color:#8b949e">Total {direction}: <b>{d["n_total"]}</b> trade |
        Rentang fokus: <b>{fmin}-{fmax}</b> ({d["n_in_focus"]} trade masuk fokus) |
        Nilai RSI{RSI_GATE_PERIOD} dgn MENANG absolut terbanyak: <b class="g">{best_s}</b> (⭐ di tabel bawah)</p>
        <table>
          <tr><th>RSI{RSI_GATE_PERIOD}</th><th>N</th><th>Menang</th><th>Kalah</th><th>WR%</th></tr>
          {per_val_rows or '<tr><td colspan="5" class="y">-</td></tr>'}
        </table>
        <p style="font-size:12px;color:#8b949e;margin-top:8px">Top 15 nilai RSI{RSI_GATE_PERIOD} diurutkan by jumlah MENANG
        terbanyak (bukan cuma WR% tertinggi, spy tidak kejebak n kecil). Nilai redup = diluar rentang fokus saat ini.</p>
        <table style="margin-top:6px">
          <tr><th>Kategori (rentang fokus {fmin}-{fmax}, dibagi rata otomatis)</th><th>N</th><th>Menang</th><th>Kalah</th><th>WR%</th></tr>
          {bucket_rows}
        </table>
        '''

    rsi_gate_html = f'''
    <h2>Analisis Khusus RSI{RSI_GATE_PERIOD} Gate — Long vs Short (Nonaktif = mode eksplorasi)</h2>
    <p class="note">💡 RSI{RSI_GATE_PERIOD} saat ini <b>{'AKTIF' if RSI_GATE_ENABLED else 'NONAKTIF'}</b> sbg gate entry —
    tabel ini menganalisis SEMUA trade yg terjadi (termasuk yg akan diblokir kalau gate diaktifkan), supaya kamu
    bisa cari rentang RSI{RSI_GATE_PERIOD} yang benar2 menguntungkan SEBELUM mengaktifkan gate-nya.
    Kolom "Kebanyakan Menang" pakai jumlah MENANG absolut (n_win), bukan WR% semata, spy tidak kejebak nilai RSI
    yang cuma muncul 1-2 kali dgn WR 100%.</p>
    <div style="display:flex; flex-wrap:wrap; gap:20px;">
      <div style="flex:1; min-width:340px;">
        <h3>🟢 LONG (Golden Cross)</h3>
        {_rsi_gate_side_html('Long')}
      </div>
      <div style="flex:1; min-width:340px;">
        <h3>🔴 SHORT (Death Cross)</h3>
        {_rsi_gate_side_html('Short')}
      </div>
    </div>
    <div class="note" style="margin-top:10px">
      ⚙️ Rentang FOKUS tabel di atas (bukan gate aktual) diatur via:
      <code>RSI_ANALYSIS_MIN_LONG</code>/<code>RSI_ANALYSIS_MAX_LONG</code> (skrg {RSI_ANALYSIS_MIN_LONG:.0f}-{RSI_ANALYSIS_MAX_LONG:.0f}),
      <code>RSI_ANALYSIS_MIN_SHORT</code>/<code>RSI_ANALYSIS_MAX_SHORT</code> (skrg {RSI_ANALYSIS_MIN_SHORT:.0f}-{RSI_ANALYSIS_MAX_SHORT:.0f}).
      Bucket Rendah/Sedang/Tinggi otomatis dibagi rata 3 dari rentang ini — ubah env var lalu jalankan
      ulang backtest utk lihat pembagian bucket yang baru.
      <br>Setelah tahu rentang yang bagus, aktifkan GATE ENTRY aktual (beda dari rentang fokus di atas)
      via Railway Variables: <code>RSI_GATE_ENABLED=1</code>, lalu atur rentang valid:
      <code>RSI_GATE_MIN_LONG</code>/<code>RSI_GATE_MAX_LONG</code> (utk Long/Golden cross),
      <code>RSI_GATE_MIN_SHORT</code>/<code>RSI_GATE_MAX_SHORT</code> (utk Short/Death cross).
      Nilai RSI{RSI_GATE_PERIOD} DILUAR rentang yg kamu set akan diblokir (sinyal dilewati, tidak entry).
      Jalankan ulang backtest setelah mengubah env var utk lihat dampaknya ke Total R & WR keseluruhan.
    </div>
    '''

    # ── Analisis Sesi Trading (WIB) & Hari — berdasarkan entry_ts (candle SAAT FILLED) ──
    sd = session_day_cp
    session_rows = ''
    session_hours = {name: (s, e) for name, s, e in _SESSION_DEFS}
    if sd.get('sessions'):
        for name, info in sd['sessions'].items():
            s_h, e_h = session_hours[name]
            wr = info['wr']
            cls = 'y' if wr is None else ('g' if wr >= 45 else ('y' if wr >= 30 else 'r'))
            wr_s = f'{wr:.1f}%' if wr is not None else '-'
            session_rows += (f'<tr><td>{name} ({s_h:02d}:00-{e_h:02d}:59 WIB)</td>'
                              f'<td>{info["n"]}</td><td class="g">{info["n_win"]}</td>'
                              f'<td class="r">{info["n_loss"]}</td><td class="{cls}">{wr_s}</td></tr>\n')
    day_rows = ''
    if sd.get('days'):
        for name in _DAY_NAMES_ID:   # urutan tetap Senin->Minggu, bukan urutan dict insert
            info = sd['days'][name]
            wr = info['wr']
            cls = 'y' if wr is None else ('g' if wr >= 45 else ('y' if wr >= 30 else 'r'))
            wr_s = f'{wr:.1f}%' if wr is not None else '-'
            day_rows += (f'<tr><td>{name}</td><td>{info["n"]}</td><td class="g">{info["n_win"]}</td>'
                          f'<td class="r">{info["n_loss"]}</td><td class="{cls}">{wr_s}</td></tr>\n')
    session_day_html = f'''
    <h2>📅 Analisis Sesi Trading (WIB) & Hari</h2>
    <p class="note">💡 Dikelompokkan berdasarkan waktu candle SAAT FILLED (entry_ts), BUKAN saat sinyal
    EMA cross muncul — mis. cross jam 13:00 WIB tapi baru filled jam 14:00 WIB akan masuk sesi London,
    bukan Asia. Semua jam dalam WIB (UTC+7).</p>
    <div style="display:flex; flex-wrap:wrap; gap:20px;">
      <div style="flex:1; min-width:320px;">
        <h3>Per Sesi Trading</h3>
        <table>
          <tr><th>Sesi</th><th>Total Trade</th><th>Menang</th><th>Kalah</th><th>WR%</th></tr>
          {session_rows or '<tr><td colspan="5" class="y">Belum ada data.</td></tr>'}
        </table>
      </div>
      <div style="flex:1; min-width:320px;">
        <h3>Per Hari</h3>
        <table>
          <tr><th>Hari</th><th>Total Trade</th><th>Menang</th><th>Kalah</th><th>WR%</th></tr>
          {day_rows or '<tr><td colspan="5" class="y">Belum ada data.</td></tr>'}
        </table>
      </div>
    </div>
    '''

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

  {filter_impact_html}

  <h2>Kontribusi Per Coin</h2>
  <div class="tbl-wrap">{coin_table}</div>

  <h2>Analisis Indikator saat Cross — Pola Menang vs Kalah</h2>
  <div class="tbl-wrap">{ind_table}</div>

  <div class="note">
    💡 Cara baca: ketiga kolom ini KUMULATIF, bukan pembagian rentang terpisah — ketiganya dievaluasi
    terhadap SEMUA trade yang sama (persis seperti menguji sebuah filter beneran):
    <br>• <b>Rendah</b> = kalau kamu HANYA ambil trade dengan nilai indikator ini ≤ t1 (kolom paling kanan),
    berapa yang menang & kalah?
    <br>• <b>Sedang</b> = kalau kamu HANYA ambil trade dengan nilai ≤ t2 (batas lebih longgar dari t1),
    berapa yang menang & kalah? (Sedang selalu MENCAKUP Rendah, karena t2 > t1 — bukan kelompok terpisah)
    <br>• <b>Tinggi</b> = SEMUA trade tanpa batas apapun — ini baseline "tanpa filter" utk pembanding.
    <br>Karena itu N di kolom Tinggi selalu = total trade keseluruhan, dan N di Rendah/Sedang biasanya
    lebih kecil (subset). Yang perlu dicari: apakah WR% naik dan jumlah <b>Menang absolut</b> tetap besar
    saat kamu perketat ke Rendah/Sedang dibanding Tinggi — kalau WR naik tapi n Menang-nya turun drastis,
    filter itu membuang lebih banyak peluang menang daripada yang disisakan.
    Bandingkan juga kolom "Avg saat WIN" vs "Avg saat LOSS" — kalau beda jauh, indikator itu berpotensi
    jadi FILTER. Kolom "t1 / t2" adalah nilai ambang aktualnya (mis. "≤1.05x / ≤1.40x").
    <br>Kalau mau menerapkan filter berdasarkan hasil ini, isi env var di Railway:
    <code>FILTER_MIN_ATR_RATIO</code>, <code>FILTER_MIN_VOL_RATIO</code>, atau
    <code>FILTER_MAX_EMA_GAP_PCT</code> (nilai ambang batas Sedang/Tinggi di atas), lalu jalankan
    ulang backtest untuk lihat dampaknya ke Total R & WR keseluruhan (bukan cuma per-bucket).
    <br>⚠️ Kolom <b>N trade</b> di Rendah/Sedang penting dicek sebelum aktifkan filter — WR tinggi di
    bucket "Rendah" tidak ada gunanya kalau isinya cuma 5 trade dari total 300 (bisa kebetulan/noise),
    dan menerapkan filter seketat itu bisa membuang mayoritas sinyal & trade menang yang sebenarnya ada.
  </div>

  {rsi_gate_html}
  {session_day_html}
  <div class="note">
    💡 Entry = LIMIT di wick candle penyebab EMA cross. SL = <b>{SL_PCT*100:.2f}%</b> dari entry
    (bukan lagi jarak struktural candle) — atur via env var <code>SL_PCT</code>.
    Support valid → bias Short, Resistance valid → bias Long (arah dibalik).
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
        'vol_ratio', 'atr_ratio', 'ema_gap_pct', 'trend_pct', 'dist_pct',
        'rsi', 'macd_hist_pct', 'sar_dist_pct', f'rsi{RSI_GATE_PERIOD}'],
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
