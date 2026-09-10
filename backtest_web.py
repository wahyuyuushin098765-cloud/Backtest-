"""
backtest_sbr.py — Backtest strategi SBR (Support/Resistance Break & Retest), H1
================================================================================
Kembali ke dasar: deteksi Level Support & Resistance lanjutan di H1, lalu jadi
sinyal SBR (Support Break & Retest) begitu level itu di-test, di-break, lalu
break-nya dikonfirmasi. Entry & manajemen presisi pakai candle M5.

RINGKASAN STRATEGI
-------------------
1) DETEKSI LEVEL (H1) — identik pola lama:
   Support: candle c1 bearish (close<open) lalu c2 bullish (close>open).
            Level = close[c1]. Valid kalau low candle c3 (setelahnya) TIDAK
            lebih rendah dari body-bottom c1/c2 (low[c3] > level).
   Resistance: kebalikannya (c1 bullish, c2 bearish). Level = close[c1].
            Valid kalau high[c3] < level.

2) JADI LEVEL "SBR" (Break & Retest) — SETELAH level terbentuk (mulai dari c3
   dan seterusnya, scan maju candle demi candle H1):
   a. TEST: minimal 1 candle yang wick-nya menyentuh level tapi CLOSE masih di
      sisi aman (support: low<=level tapi close>level). Boleh lebih dari 1x.
   b. BREAK: candle yang closenya menembus level (support: close<level).
   c. KONFIRMASI: candle TEPAT SETELAH candle break, wick-nya TIDAK balik
      menyentuh level itu lagi (support: low candle konfirmasi > level).
   Begitu (a),(b),(c) semua terpenuhi -> level jadi SBR AKTIF (arah SHORT utk
   support, LONG utk resistance), disimpan dgn status 'menunggu harga mendekat'.

3) TRIGGER ENTRY (dipantau di candle M5, presisi) — SETELAH SBR aktif:
   Selama level belum dipakai, tiap candle M5 dicek jaraknya ke level:
     - Kalau harga masuk radius 2% dari level -> limit order dipasang PERSIS
       di level itu (arah short utk support, long utk resistance).
     - Selama limit terpasang, kalau wick M5 menyentuh level -> FILL persis
       di level (harga limit).
     - Kalau sebelum fill harga malah menjauh lagi >2% dari level -> limit
       DIBATALKAN (order dicabut), tapi level TETAP tersimpan aktif -- bisa
       terpasang ulang nanti kalau harga mendekat lagi dalam radius 2%.
   SL = SL_PCT (default 1%) dari harga entry, arah berlawanan dari entry.
   Level MATI (tidak dipakai lagi) setelah 1x FILLED (menang ataupun kalah).

Deploy ke Railway:
  Start command -> python backtest_sbr.py
  Buka domain Railway -> lihat progress & hasil di browser (auto-refresh)
"""

import os, threading, time, io, csv
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from pybit.unified_trading import HTTP

# ============================================================
# CONFIG (override via environment variable kalau perlu)
# ============================================================
PORT             = int(os.environ.get('PORT', 8080))
INITIAL_BALANCE  = float(os.environ.get('INITIAL_BALANCE', '30.0'))   # modal awal, 1 akun bersama
RISK_PCT         = float(os.environ.get('RISK_PCT', '0.01'))          # risk 1% balance/trade (compound)
FEE_ENTRY_PCT    = float(os.environ.get('FEE_ENTRY_PCT', '0.00055'))
FEE_EXIT_PCT     = float(os.environ.get('FEE_EXIT_PCT', str(0.00055 * 3)))

SL_PCT           = float(os.environ.get('SL_PCT', '0.01'))            # SL fix 1% dari entry (=1R)
TRAIL_ACTIVATE_R = float(os.environ.get('TRAIL_ACTIVATE_R', '3.0'))    # trailing aktif begitu profit capai 3R
TRAIL_STOP_R     = float(os.environ.get('TRAIL_STOP_R', '1.0'))       # setelah aktif, SL mengikuti 1R di belakang harga tertinggi/terendah
APPROACH_PCT     = float(os.environ.get('APPROACH_PCT', '0.02'))      # radius 2% utk pasang/cabut limit

LEVERAGE           = float(os.environ.get('LEVERAGE', '50'))
MARGIN_USAGE_CAP    = float(os.environ.get('MARGIN_USAGE_CAP', '0.90'))

MAX_CONCURRENT_RAW = os.environ.get('MAX_CONCURRENT', '0').strip().lower()
MAX_CONCURRENT = float('inf') if MAX_CONCURRENT_RAW in ('', '0', 'unlimited', 'inf') else int(MAX_CONCURRENT_RAW)

ALLOW_HEDGE      = os.environ.get('ALLOW_HEDGE', 'true').lower() == 'true'

SIMULATE_MIN_ORDER  = os.environ.get('SIMULATE_MIN_ORDER', '1').strip() not in ('0', 'false', 'False', '')
MIN_ORDER_USD       = float(os.environ.get('MIN_ORDER_USD', '5.0'))
ORDER_BUMP_FLOOR     = float(os.environ.get('ORDER_BUMP_FLOOR', '4.0'))
QTY_STEP_APPROX      = float(os.environ.get('QTY_STEP_APPROX', '0.000001'))

BACKTEST_START_DATE = os.environ.get('BACKTEST_START_DATE', '2025-08-01')
BACKTEST_END_DATE   = os.environ.get('BACKTEST_END_DATE', '2026-07-31')

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


def _apply_min_order_size(raw_qty, entry_p):
    if not SIMULATE_MIN_ORDER:
        return raw_qty, False, False
    step = QTY_STEP_APPROX
    qty = round(raw_qty / step) * step
    if qty < step:
        return 0, True, False
    order_value = qty * entry_p
    if order_value < MIN_ORDER_USD:
        if order_value >= ORDER_BUMP_FLOOR:
            qty = round((MIN_ORDER_USD / entry_p) / step) * step
            if qty * entry_p < MIN_ORDER_USD:
                qty += step
            return qty, False, True
        else:
            return 0, True, False
    return qty, False, False


def _date_to_ms(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

_START_MS = _date_to_ms(BACKTEST_START_DATE)
_END_MS   = _date_to_ms(BACKTEST_END_DATE) + 86400 * 1000 - 1

# ============================================================
# GLOBAL STATE
# ============================================================
_lock       = threading.Lock()
_log        = []
_phase      = 'running'
_results    = []
_kind_results = []
_all_trades = []
_combined_result = {
    'n_trades': 0, 'n_win': 0, 'n_loss': 0, 'wr': 0, 'total_pnl': 0, 'roi': 0,
    'total_r': 0, 'avg_r': 0, 'final_balance': INITIAL_BALANCE,
    'blocked_by_slot': 0, 'blocked_by_margin': 0, 'blocked_by_min_order': 0,
}


def _ts():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%H:%M:%S')

def _log_msg(msg: str):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    with _lock:
        _log.append(line)


# ============================================================
# FETCH DATA DARI BYBIT (dgn CACHE ke disk)
# ============================================================

def _cache_path(symbol: str, tf: str) -> str:
    return os.path.join(CACHE_DIR, f"{symbol}_{tf}_{BACKTEST_START_DATE}_{BACKTEST_END_DATE}.csv")


def _load_cache(symbol: str, tf: str):
    path = _cache_path(symbol, tf)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if not df.empty and {'ts', 'open', 'high', 'low', 'close', 'vol'}.issubset(df.columns):
                return df
        except Exception as e:
            _log_msg(f"   ⚠ {symbol} {tf}: cache korup ({e}), fetch ulang dari Bybit.")
    return None


def _save_cache(symbol: str, tf: str, df: pd.DataFrame):
    try:
        df.to_csv(_cache_path(symbol, tf), index=False)
    except Exception as e:
        _log_msg(f"   ⚠ {symbol} {tf}: gagal simpan cache — {e}")


def fetch_bybit(symbol: str, interval, tf: str) -> pd.DataFrame:
    """interval: 60 utk H1, 5 utk M5 (parameter get_kline Bybit)."""
    cached = _load_cache(symbol, tf)
    if cached is not None:
        _log_msg(f"   💾 {symbol} {tf}: pakai cache ({len(cached):,} candle) — skip fetch Bybit.")
        return cached

    session = HTTP(testnet=False)
    rows, cur_end, n_call = [], _END_MS, 0
    while True:
        for attempt in range(4):
            try:
                res = session.get_kline(symbol=symbol, category='linear', interval=interval,
                                        limit=1000, start=_START_MS, end=cur_end)
                data = res['result']['list']
                break
            except Exception as e:
                wait = 2 ** attempt
                _log_msg(f"   ⚠ {symbol} {tf} API error (attempt {attempt+1}): {e} — retry {wait}s")
                time.sleep(wait)
        else:
            _log_msg(f"   ❌ {symbol} {tf}: gagal fetch setelah 4 percobaan.")
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
    _save_cache(symbol, tf, df)
    return df


def fetch_bybit_h1(symbol: str) -> pd.DataFrame:
    return fetch_bybit(symbol, 60, 'H1')


def fetch_bybit_m5(symbol: str) -> pd.DataFrame:
    return fetch_bybit(symbol, 5, 'M5')


# ============================================================
# DETEKSI LEVEL SUPPORT / RESISTANCE (H1)
# ============================================================

def find_levels(df):
    """Deteksi level S/R dasar dari candle H1 (basis body candle, sama pola lama).
    Syarat kanan: candle c3 (setelah c1,c2) tidak boleh menembus body level.
    Syarat kiri : candle SEBELUM c1 (index c1-1) juga tidak boleh menembus body
                  level -- kalau c1 adalah candle paling awal (tidak ada candle
                  sebelumnya), level GUGUR.
    Return list dict: {'type': 'support'/'resistance', 'level': harga, 'c1', 'c2', 'c3'}."""
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    n = len(df)
    levels = []
    for i in range(1, n - 2):   # mulai dari 1, supaya selalu ada candle sebelum c1
        if c[i] < o[i] and c[i + 1] > o[i + 1]:          # bearish lalu bullish
            S = c[i]
            if l[i + 2] > S + 1e-9 and l[i - 1] > S + 1e-9:   # kanan (c3) DAN kiri (c1-1) tidak menembus
                levels.append({'type': 'support', 'level': S, 'c1': i, 'c2': i + 1, 'c3': i + 2})
        if c[i] > o[i] and c[i + 1] < o[i + 1]:          # bullish lalu bearish
            R = c[i]
            if h[i + 2] < R - 1e-9 and h[i - 1] < R - 1e-9:   # kanan (c3) DAN kiri (c1-1) tidak menembus
                levels.append({'type': 'resistance', 'level': R, 'c1': i, 'c2': i + 1, 'c3': i + 2})
    return levels


# ============================================================
# DETEKSI SBR & RBS (Support/Resistance Break & Retest)
# ============================================================
# SBR = Support jadi Resistance (arah entry: Short)
# RBS = Resistance jadi Support (arah entry: Long)
#
# Untuk tiap level (mulai scan dari candle c3), cari SEKALI kejadian pertama:
#   TEST   -> minimal 1 candle wick menyentuh level tapi close masih aman
#   BREAK  -> candle pertama SETELAH test yang close-nya menembus level
#   KONFIRM-> candle TEPAT SETELAH break, wick-nya TIDAK balik menyentuh level
#
# Catatan urutan: TEST harus terjadi SEBELUM BREAK (candle test dan candle
# break boleh saja candle yang sama SELAMA candle itu masih "test" dulu di
# baca -- tapi karena test butuh close aman & break butuh close tembus, satu
# candle tidak bisa jadi TEST dan BREAK sekaligus, jadi otomatis break selalu
# candle yang berbeda dan setelah test pertama).

def detect_sbr_rbs_events(df):
    """Return list dict SBR/RBS aktif:
    {'kind': 'SBR'/'RBS', 'type': support/resistance, 'level': harga,
     'direction': Short/Long, 'break_i', 'confirm_i', 'confirm_ts', 'c1', 'c2'}
    """
    ts = df['ts'].values
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    n = len(df)
    levels = find_levels(df)
    events = []

    for lv in levels:
        level = lv['level']
        ty = lv['type']
        start = lv['c3']  # mulai scan dari candle c3 (candle pertama setelah level terbentuk)

        tested = False
        break_i = None
        invalid = False
        # cari TEST dulu, lalu BREAK setelah test. Kalau BREAK terjadi SEBELUM
        # ada test valid, level ini GUGUR TOTAL (tidak dicoba lagi).
        i = start
        while i < n:
            if ty == 'support':
                if not tested:
                    if l[i] <= level + 1e-9 and c[i] > level + 1e-9:
                        tested = True
                        i += 1
                        continue
                    if c[i] < level - 1e-9:
                        invalid = True   # break duluan sebelum ada test -> gugur
                        break
                    i += 1
                    continue
                # sudah tested -> cari BREAK (close menembus ke bawah)
                if c[i] < level - 1e-9:
                    break_i = i
                    break
                i += 1
            else:  # resistance
                if not tested:
                    if h[i] >= level - 1e-9 and c[i] < level - 1e-9:
                        tested = True
                        i += 1
                        continue
                    if c[i] > level + 1e-9:
                        invalid = True
                        break
                    i += 1
                    continue
                if c[i] > level + 1e-9:
                    break_i = i
                    break
                i += 1

        if invalid or break_i is None or break_i + 1 >= n:
            continue  # gugur (break sebelum test), tidak ada break, atau break di candle terakhir

        confirm_i = break_i + 1
        if ty == 'support':
            # break ke bawah -> retest gagal berarti wick ATAS tidak balik naik ke level
            wick_ok = h[confirm_i] < level - 1e-9
            # DAN body candle konfirmasi harus searah break (bearish: close < open)
            body_ok = c[confirm_i] < o[confirm_i] - 1e-9
        else:
            # break ke atas -> retest gagal berarti wick BAWAH tidak balik turun ke level
            wick_ok = l[confirm_i] > level + 1e-9
            # DAN body candle konfirmasi harus searah break (bullish: close > open)
            body_ok = c[confirm_i] > o[confirm_i] + 1e-9

        confirmed = wick_ok and body_ok

        if not confirmed:
            continue

        kind = 'SBR' if ty == 'support' else 'RBS'
        direction = 'Short' if ty == 'support' else 'Long'
        events.append({
            'kind': kind, 'type': ty, 'level': level, 'direction': direction,
            'break_i': break_i, 'confirm_i': confirm_i,
            'confirm_ts': int(ts[confirm_i]),
            'c1': lv['c1'], 'c2': lv['c2'],
        })

    events.sort(key=lambda e: e['confirm_ts'])
    return events


# ============================================================
# DETEKSI QMS & QMR (Quasimodo Support / Resistance)
# ============================================================
# QMS = level SUPPORT di-break ke bawah (BREAK-1), lalu candle BERIKUTNYA
#       balik break ke ATAS di level yang SAMA (BREAK-2), lalu candle
#       setelahnya (KONFIRMASI) low-nya TIDAK menyentuh level lagi.
#       Entry: LONG di level (support awal yang jadi acuan).
# QMR = kebalikannya: level RESISTANCE di-break ke atas (BREAK-1), lalu
#       candle berikutnya balik break ke BAWAH (BREAK-2), konfirmasi
#       high-nya TIDAK menyentuh level lagi. Entry: SHORT.
#
# TEST tetap wajib sebelum BREAK-1 (sama seperti SBR/RBS): minimal 1 candle
# wick menyentuh level dengan close masih aman, sebelum break pertama terjadi.

def detect_qm_events(df):
    """Return list dict QMS/QMR aktif:
    {'kind': 'QMS'/'QMR', 'type': support/resistance, 'level': harga,
     'direction': Long/Short, 'break1_i', 'break2_i', 'confirm_i', 'confirm_ts',
     'c1', 'c2'}
    """
    ts = df['ts'].values
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    n = len(df)
    levels = find_levels(df)
    events = []

    for lv in levels:
        level = lv['level']
        ty = lv['type']
        start = lv['c3']

        tested = False
        break1_i = None
        invalid = False
        i = start
        while i < n:
            if ty == 'support':
                if not tested:
                    if l[i] <= level + 1e-9 and c[i] > level + 1e-9:
                        tested = True
                        i += 1
                        continue
                    if c[i] < level - 1e-9:
                        invalid = True   # break duluan sebelum ada test -> gugur
                        break
                    i += 1
                    continue
                if c[i] < level - 1e-9:   # BREAK-1 ke bawah
                    break1_i = i
                    break
                i += 1
            else:  # resistance
                if not tested:
                    if h[i] >= level - 1e-9 and c[i] < level - 1e-9:
                        tested = True
                        i += 1
                        continue
                    if c[i] > level + 1e-9:
                        invalid = True
                        break
                    i += 1
                    continue
                if c[i] > level + 1e-9:   # BREAK-1 ke atas
                    break1_i = i
                    break
                i += 1

        if invalid or break1_i is None or break1_i + 1 >= n:
            continue

        break2_i = break1_i + 1
        if ty == 'support':
            # BREAK-2: candle tepat setelah break1 balik close di ATAS level
            break2_ok = c[break2_i] > level + 1e-9
        else:
            # BREAK-2: candle tepat setelah break1 balik close di BAWAH level
            break2_ok = c[break2_i] < level - 1e-9

        if not break2_ok or break2_i + 1 >= n:
            continue

        confirm_i = break2_i + 1
        if ty == 'support':
            # QMS -> entry Long: wick TIDAK menyentuh level, DAN body searah (bullish)
            wick_ok = l[confirm_i] > level + 1e-9
            body_ok = c[confirm_i] > o[confirm_i] + 1e-9
        else:
            # QMR -> entry Short: wick TIDAK menyentuh level, DAN body searah (bearish)
            wick_ok = h[confirm_i] < level - 1e-9
            body_ok = c[confirm_i] < o[confirm_i] - 1e-9

        confirmed = wick_ok and body_ok

        if not confirmed:
            continue

        kind = 'QMS' if ty == 'support' else 'QMR'
        direction = 'Long' if ty == 'support' else 'Short'
        events.append({
            'kind': kind, 'type': ty, 'level': level, 'direction': direction,
            'break1_i': break1_i, 'break2_i': break2_i, 'confirm_i': confirm_i,
            'confirm_ts': int(ts[confirm_i]),
            'c1': lv['c1'], 'c2': lv['c2'],
        })

    events.sort(key=lambda e: e['confirm_ts'])
    return events


def detect_all_events(df):
    """Gabungan SBR + RBS + QMS + QMR, urut by confirm_ts.
    Dedup: 2 event dgn (kind, level, confirm_ts) SAMA PERSIS dianggap 1 sinyal
    yg sama (bisa terjadi kalau 2 basis candle c1/c2 berbeda kebetulan
    menghasilkan level & window break/confirm yg identik) -- ambil salah satu
    saja supaya tidak dihitung 2x atau collision key posisi trading."""
    events = detect_sbr_rbs_events(df) + detect_qm_events(df)
    seen = set()
    deduped = []
    for e in events:
        dedup_key = (e['kind'], round(e['level'], 10), e['confirm_ts'])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        deduped.append(e)
    deduped.sort(key=lambda e: e['confirm_ts'])
    return deduped


# ============================================================
# STRUKTUR M5 UTK LOOKUP CEPAT
# ============================================================

def prepare_m5(df_m5):
    if df_m5 is None or df_m5.empty:
        return None
    df_m5 = df_m5.sort_values('ts').reset_index(drop=True)
    return {
        'TS': df_m5['ts'].values.astype(np.int64),
        'O': df_m5['open'].values, 'H': df_m5['high'].values,
        'L': df_m5['low'].values, 'C': df_m5['close'].values,
    }


# ============================================================
# PERSIAPAN PER-KOIN
# ============================================================

def prepare_coin(symbol, df):
    df = df.sort_values('ts').reset_index(drop=True)
    events = detect_all_events(df)
    return {
        'symbol': symbol,
        'TS': df['ts'].values.astype(np.int64),
        'events': events,   # list SBR, urut by confirm_ts
        'n': len(df),
    }


# ============================================================
# SIMULASI GABUNGAN (semua koin, 1 balance, 1 pool slot)
# ============================================================
#
# Alur per koin, per level SBR aktif (mulai dipantau setelah confirm_ts):
#   Level punya status: 'waiting' (belum ada limit terpasang) atau
#   'armed' (limit sudah terpasang, menunggu fill / batal).
#   Dipantau candle M5 demi candle M5 SETELAH confirm_ts:
#     - waiting: kalau jarak harga (close M5) ke level <= APPROACH_PCT -> ARM
#       (pasang limit persis di level).
#     - armed: kalau wick M5 menyentuh level -> FILL (entry = level).
#              kalau jarak harga menjauh > APPROACH_PCT lagi (belum fill)
#              -> DISARM (limit dicabut, balik ke 'waiting', level tetap hidup).
#   Begitu FILLED, level MATI (tidak dipantau lagi baik menang/kalah).

def run_combined_backtest(coins: dict, m5_data: dict) -> dict:
    balance = INITIAL_BALANCE
    active_positions = {}     # key(symbol,direction+level) -> {...}
    trades = []
    blocked_by_slot = 0
    blocked_by_margin = 0
    blocked_by_min_order = 0

    def _akey(symbol, direction, level, kind):
        return f"{symbol}|{direction}|{kind}|{level:.10f}"

    total_margin_used = 0.0   # dijaga incremental, bukan sum() ulang tiap panggilan (O(1) bukan O(n))

    def _slots_used():
        return len(active_positions)

    positions_by_symbol = {}   # symbol -> set(keys)

    level_state = {}   # (symbol, idx_event) -> {'status', 'used'}
    for symbol, cp in coins.items():
        for idx, ev in enumerate(cp['events']):
            level_state[(symbol, idx)] = {'status': 'waiting', 'used': False}

    # pending_activation: level yg confirm_ts-nya BELUM lewat, urut asc per simbol
    pending_activation = {}
    live_levels_by_symbol = {symbol: [] for symbol in coins}
    for symbol, cp in coins.items():
        pending_activation[symbol] = sorted(
            [(ev['confirm_ts'], idx) for idx, ev in enumerate(cp['events'])])

    def open_trade(symbol, ev, entry_price, entry_ts):
        nonlocal balance, total_margin_used
        direction = ev['direction']
        if direction == 'Short':
            sl = entry_price * (1 + SL_PCT)
        else:
            sl = entry_price * (1 - SL_PCT)
        dist = abs(entry_price - sl)   # = 1R

        risk_amount = balance * RISK_PCT
        raw_qty = risk_amount / dist if dist > 0 else 0
        qty, skipped, bumped = _apply_min_order_size(raw_qty, entry_price)
        if skipped or qty <= 0:
            return None, 'min_order'

        notional = entry_price * qty
        margin_needed = notional / LEVERAGE
        if (total_margin_used + margin_needed) > balance * MARGIN_USAGE_CAP:
            return None, 'margin'

        if _slots_used() >= MAX_CONCURRENT:
            return None, 'slot'

        key = _akey(symbol, direction, ev['level'], ev['kind'])
        active_positions[key] = {
            'symbol': symbol, 'direction': direction, 'entry': entry_price, 'sl': sl,
            'dist': dist, 'qty': qty, 'entry_ts': entry_ts, 'level': ev['level'],
            'kind': ev['kind'], 'margin': margin_needed,
            'trail_active': False, 'extreme': entry_price,   # high/low-water mark, mulai dari entry
        }
        total_margin_used += margin_needed
        positions_by_symbol.setdefault(symbol, set()).add(key)
        return key, None

    def close_trade(key, exit_price, reason, exit_ts):
        nonlocal balance, total_margin_used
        pos = active_positions.pop(key)
        positions_by_symbol.get(pos['symbol'], set()).discard(key)
        total_margin_used -= pos['margin']
        entry, dist, qty, direction = pos['entry'], pos['dist'], pos['qty'], pos['direction']
        pnl_gross = (exit_price - entry) * qty if direction == 'Long' else (entry - exit_price) * qty
        fee = entry * qty * FEE_ENTRY_PCT + exit_price * qty * FEE_EXIT_PCT
        pnl_net = pnl_gross - fee
        balance += pnl_net
        r_mult = pnl_net / (dist * qty) if dist * qty > 0 else 0
        trades.append({
            'symbol': pos['symbol'], 'direction': direction, 'entry': entry, 'sl': pos['sl'],
            'exit': exit_price, 'reason': reason, 'r_mult': r_mult, 'pnl_usd': pnl_net,
            'entry_ts': pos['entry_ts'], 'exit_ts': exit_ts, 'balance_after': balance,
            'level': pos['level'], 'kind': pos['kind'],
        })

    def process_symbol_tick(symbol, j):
        """Proses SEMUA hal (exit posisi & level live) untuk 1 simbol di index candle j."""
        m5 = m5_data[symbol]
        now_ts = int(m5['TS'][j])
        hi, lo, close_p = m5['H'][j], m5['L'][j], m5['C'][j]

        # 1) exit posisi aktif simbol ini (dgn trailing stop 1:3 aktivasi, 1R trailing)
        keys = positions_by_symbol.get(symbol)
        if keys:
            for key in list(keys):
                pos = active_positions[key]
                direction = pos['direction']
                entry, dist = pos['entry'], pos['dist']

                if direction == 'Long':
                    # update high-water mark & cek aktivasi pakai HIGH candle (best-case dulu)
                    if hi > pos['extreme']:
                        pos['extreme'] = hi
                    profit_r = (pos['extreme'] - entry) / dist
                    if not pos['trail_active'] and profit_r >= TRAIL_ACTIVATE_R:
                        pos['trail_active'] = True
                    if pos['trail_active']:
                        new_sl = pos['extreme'] - TRAIL_STOP_R * dist
                        if new_sl > pos['sl']:
                            pos['sl'] = new_sl   # SL cuma boleh naik (menguntungkan), tak pernah mundur
                    # cek SL kena pakai LOW candle (worst-case, setelah SL di-update)
                    if lo <= pos['sl'] + 1e-12:
                        close_trade(key, pos['sl'], 'SL' if not pos['trail_active'] else 'TRAIL', now_ts)
                else:  # Short
                    if lo < pos['extreme']:
                        pos['extreme'] = lo
                    profit_r = (entry - pos['extreme']) / dist
                    if not pos['trail_active'] and profit_r >= TRAIL_ACTIVATE_R:
                        pos['trail_active'] = True
                    if pos['trail_active']:
                        new_sl = pos['extreme'] + TRAIL_STOP_R * dist
                        if new_sl < pos['sl']:
                            pos['sl'] = new_sl
                    if hi >= pos['sl'] - 1e-12:
                        close_trade(key, pos['sl'], 'SL' if not pos['trail_active'] else 'TRAIL', now_ts)

        # 2) level live simbol ini: waiting -> armed -> fill
        cp = coins[symbol]
        live_idxs = live_levels_by_symbol.get(symbol)
        if live_idxs:
            still_live = []
            for idx in live_idxs:
                st = level_state[(symbol, idx)]
                if st['used']:
                    continue   # sudah dipakai -> dibuang dari daftar live (tidak scan lagi)
                ev = cp['events'][idx]
                level = ev['level']

                dist_pct = abs(close_p - level) / level
                if st['status'] == 'waiting':
                    if dist_pct <= APPROACH_PCT:
                        st['status'] = 'armed'
                elif st['status'] == 'armed':
                    touched = (lo <= level <= hi)
                    if touched:
                        opened_key, block_reason = open_trade(symbol, ev, level, now_ts)
                        if opened_key is not None:
                            st['used'] = True
                        else:
                            nonlocal_blocks[block_reason] += 1
                    elif dist_pct > APPROACH_PCT:
                        st['status'] = 'waiting'
                if not st['used']:
                    still_live.append(idx)
            live_levels_by_symbol[symbol] = still_live

    nonlocal_blocks = {'slot': 0, 'margin': 0, 'min_order': 0}

    # ── TIMELINE EFISIEN via K-WAY MERGE (pointer index, bukan searchsorted) ──
    # Tiap simbol punya pointer int ke posisi candle M5 berikutnya yg BELUM
    # diproses. Heap cuma menyimpan (ts_candle_berikutnya, symbol) utk tahu
    # simbol mana yg harus diproses duluan (urutan kronologis lintas simbol,
    # perlu utk shared balance/margin/slot). Advance pointer = O(1) (i+=1),
    # bukan np.searchsorted (O(log n) tapi overhead call besar tiap tick).
    import heapq

    ptr = {symbol: 0 for symbol in coins}   # pointer index candle M5 berikutnya per simbol
    m5_ts_arr = {symbol: m5['TS'] for symbol, m5 in m5_data.items() if m5 is not None}
    m5_len = {symbol: len(arr) for symbol, arr in m5_ts_arr.items()}

    # simbol mulai diproses dari pointer candle M5 pertama SETELAH confirm_ts level pertamanya
    def _advance_ptr_to(symbol, target_ts):
        """Majukan ptr[symbol] sampai candle M5 pertama dgn ts >= target_ts (linear, tapi
        dipanggil jarang -- hanya saat lompat jauh, bukan tiap tick normal)."""
        arr = m5_ts_arr.get(symbol)
        if arr is None:
            return
        i = ptr[symbol]
        n = m5_len[symbol]
        while i < n and arr[i] < target_ts:
            i += 1
        ptr[symbol] = i

    heap = []   # (ts, symbol) -- symbol siap diproses di candle ptr[symbol]

    for symbol, cp in coins.items():
        if not cp['events'] or symbol not in m5_ts_arr:
            continue
        first_confirm = cp['events'][0]['confirm_ts']
        _advance_ptr_to(symbol, first_confirm + 1)
        if ptr[symbol] < m5_len[symbol]:
            heapq.heappush(heap, (int(m5_ts_arr[symbol][ptr[symbol]]), symbol))

    while heap:
        now_ts, symbol = heapq.heappop(heap)
        i = ptr[symbol]
        if i >= m5_len[symbol] or m5_ts_arr[symbol][i] != now_ts:
            continue   # stale entry (seharusnya tidak terjadi, safety check)

        # aktifkan level yg confirm_ts-nya sudah lewat now_ts
        plist = pending_activation.get(symbol)
        if plist:
            while plist and plist[0][0] < now_ts:
                _, idx = plist.pop(0)
                live_levels_by_symbol[symbol].append(idx)

        process_symbol_tick(symbol, i)

        # advance pointer: kalau simbol masih 'aktif' (posisi/level live), lanjut
        # candle BERIKUTNYA (i+1, O(1)). Kalau tidak ada apa2 yg live, lompat
        # jauh ke confirm_ts level pending berikutnya (hemat banyak tick).
        has_active = bool(positions_by_symbol.get(symbol)) or bool(live_levels_by_symbol.get(symbol))
        if has_active:
            ptr[symbol] = i + 1
        else:
            plist = pending_activation.get(symbol)
            if plist:
                _advance_ptr_to(symbol, plist[0][0] + 1)
            else:
                ptr[symbol] = m5_len[symbol]   # tidak ada level pending lagi -> selesai

        if ptr[symbol] < m5_len[symbol]:
            heapq.heappush(heap, (int(m5_ts_arr[symbol][ptr[symbol]]), symbol))

    blocked_by_slot = nonlocal_blocks['slot']
    blocked_by_margin = nonlocal_blocks['margin']
    blocked_by_min_order = nonlocal_blocks['min_order']

    n_trades = len(trades)
    n_win = sum(1 for t in trades if t['pnl_usd'] > 0)
    n_loss = n_trades - n_win
    wr = (n_win / n_trades * 100) if n_trades else 0
    total_pnl = sum(t['pnl_usd'] for t in trades)
    total_r = sum(t['r_mult'] for t in trades)
    avg_r = total_r / n_trades if n_trades else 0
    roi = (balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100

    return {
        'trades': trades, 'n_trades': n_trades, 'n_win': n_win, 'n_loss': n_loss,
        'wr': wr, 'total_pnl': total_pnl, 'total_r': total_r, 'avg_r': avg_r,
        'final_balance': balance, 'roi': roi,
        'blocked_by_slot': blocked_by_slot, 'blocked_by_margin': blocked_by_margin,
        'blocked_by_min_order': blocked_by_min_order,
    }


# ============================================================
# BREAKDOWN PER SIMBOL
# ============================================================

def per_symbol_breakdown(trades):
    by_symbol = {}
    for t in trades:
        s = t['symbol']
        d = by_symbol.setdefault(s, {'n': 0, 'win': 0, 'total_r': 0.0, 'total_pnl': 0.0})
        d['n'] += 1
        if t['pnl_usd'] > 0:
            d['win'] += 1
        d['total_r'] += t['r_mult']
        d['total_pnl'] += t['pnl_usd']
    rows = []
    for s, d in by_symbol.items():
        wr = d['win'] / d['n'] * 100 if d['n'] else 0
        rows.append({'symbol': s, 'n': d['n'], 'win': d['win'], 'wr': wr,
                     'total_r': d['total_r'], 'total_pnl': d['total_pnl']})
    rows.sort(key=lambda r: -r['total_r'])
    return rows


def per_kind_breakdown(trades):
    """Breakdown performa per JENIS level: SBR / RBS / QMS / QMR."""
    by_kind = {}
    for t in trades:
        k = t.get('kind', '?')
        d = by_kind.setdefault(k, {'n': 0, 'win': 0, 'total_r': 0.0, 'total_pnl': 0.0})
        d['n'] += 1
        if t['pnl_usd'] > 0:
            d['win'] += 1
        d['total_r'] += t['r_mult']
        d['total_pnl'] += t['pnl_usd']
    rows = []
    for k, d in by_kind.items():
        wr = d['win'] / d['n'] * 100 if d['n'] else 0
        rows.append({'kind': k, 'n': d['n'], 'win': d['win'], 'wr': wr,
                     'total_r': d['total_r'], 'total_pnl': d['total_pnl']})
    order = {'SBR': 0, 'RBS': 1, 'QMS': 2, 'QMR': 3}
    rows.sort(key=lambda r: order.get(r['kind'], 99))
    return rows


# ============================================================
# BACKGROUND WORKER
# ============================================================

def _run():
    global _phase, _results, _kind_results, _all_trades, _combined_result
    try:
        _log_msg(f"🚀 Mulai backtest SBR/RBS/QMS/QMR — {len(SYMBOLS)} koin, {BACKTEST_START_DATE} s/d {BACKTEST_END_DATE}")
        _log_msg(f"   SL={SL_PCT*100:.2f}% (=1R)  Trailing: aktif di {TRAIL_ACTIVATE_R:.1f}R, "
                  f"jarak {TRAIL_STOP_R:.1f}R dari extreme  APPROACH_PCT={APPROACH_PCT*100:.1f}%")

        coins = {}
        m5_data = {}
        for symbol in SYMBOLS:
            _log_msg(f"📊 {symbol}: fetch H1...")
            df_h1 = fetch_bybit_h1(symbol)
            if df_h1.empty:
                _log_msg(f"   ⚠ {symbol}: data H1 kosong, skip.")
                continue
            _log_msg(f"📊 {symbol}: fetch M5...")
            df_m5 = fetch_bybit_m5(symbol)
            cp = prepare_coin(symbol, df_h1)
            coins[symbol] = cp
            m5_data[symbol] = prepare_m5(df_m5)
            n_by_kind = {}
            for ev in cp['events']:
                n_by_kind[ev['kind']] = n_by_kind.get(ev['kind'], 0) + 1
            kind_str = ', '.join(f"{k}:{v}" for k, v in sorted(n_by_kind.items()))
            _log_msg(f"   ✅ {symbol}: {cp['n']} candle H1, {len(cp['events'])} level terdeteksi ({kind_str}).")

        _log_msg(f"🧮 Menjalankan simulasi gabungan ({len(coins)} koin)...")
        result = run_combined_backtest(coins, m5_data)

        with _lock:
            _all_trades[:] = result['trades']
            _combined_result.update({
                'n_trades': result['n_trades'], 'n_win': result['n_win'], 'n_loss': result['n_loss'],
                'wr': result['wr'], 'total_pnl': result['total_pnl'], 'roi': result['roi'],
                'total_r': result['total_r'], 'avg_r': result['avg_r'],
                'final_balance': result['final_balance'],
                'blocked_by_slot': result['blocked_by_slot'],
                'blocked_by_margin': result['blocked_by_margin'],
                'blocked_by_min_order': result['blocked_by_min_order'],
            })
            _results[:] = per_symbol_breakdown(result['trades'])
            _kind_results[:] = per_kind_breakdown(result['trades'])
            _phase = 'done'

        _log_msg(f"✅ SELESAI. {result['n_trades']} trade, WR {result['wr']:.1f}%, "
                  f"Total R {result['total_r']:.2f}, Balance akhir ${result['final_balance']:.2f} "
                  f"(ROI {result['roi']:+.1f}%)")
    except Exception as e:
        import traceback
        _log_msg(f"❌ ERROR: {e}")
        _log_msg(traceback.format_exc())
        with _lock:
            _phase = 'error'


# ============================================================
# DASHBOARD HTML
# ============================================================

def _fmt_max_concurrent():
    return 'Tanpa batas' if MAX_CONCURRENT == float('inf') else str(MAX_CONCURRENT)


def _render_html() -> bytes:
    with _lock:
        phase = _phase
        cr = dict(_combined_result)
        results_cp = list(_results)
        kind_cp = list(_kind_results)
        log_cp = list(_log[-300:])

    log_html = '\n'.join(l for l in log_cp)

    if phase == 'running':
        status_html = '<div class="status running">⏳ Sedang berjalan...</div>'
    elif phase == 'error':
        status_html = '<div class="status error">❌ Terjadi error — lihat log di bawah.</div>'
    else:
        status_html = '<div class="status done">✅ Selesai</div>'

    rows_html = ''
    for r in results_cp:
        cls = 'pos' if r['total_r'] >= 0 else 'neg'
        rows_html += f'''<tr>
            <td>{r['symbol']}</td><td>{r['n']}</td><td>{r['win']}</td>
            <td>{r['wr']:.1f}%</td><td class="{cls}">{r['total_r']:+.2f}</td>
            <td class="{cls}">${r['total_pnl']:+.2f}</td></tr>'''

    kind_rows_html = ''
    for r in kind_cp:
        cls = 'pos' if r['total_r'] >= 0 else 'neg'
        kind_rows_html += f'''<tr>
            <td>{r['kind']}</td><td>{r['n']}</td><td>{r['win']}</td>
            <td>{r['wr']:.1f}%</td><td class="{cls}">{r['total_r']:+.2f}</td>
            <td class="{cls}">${r['total_pnl']:+.2f}</td></tr>'''

    return f'''<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<title>Backtest SBR/RBS/QMS/QMR</title>
<style>
  body {{ font-family: -apple-system, Arial, sans-serif; background:#0f1117; color:#e6e6e6; margin:0; padding:20px; }}
  h1 {{ font-size:20px; }}
  h2 {{ font-size:16px; margin-top:28px; }}
  .status {{ padding:10px 14px; border-radius:8px; margin-bottom:16px; font-weight:600; }}
  .status.running {{ background:#3a2f00; color:#ffd866; }}
  .status.done {{ background:#0f3a1e; color:#7ee787; }}
  .status.error {{ background:#3a0f0f; color:#ff7b72; }}
  .cards {{ display:flex; flex-wrap:wrap; gap:12px; margin-bottom:20px; }}
  .card {{ background:#161b22; border:1px solid #30363d; border-radius:10px; padding:14px 18px; min-width:140px; }}
  .card .label {{ font-size:12px; color:#8b949e; }}
  .card .value {{ font-size:22px; font-weight:700; margin-top:4px; }}
  table {{ border-collapse: collapse; width:100%; font-size:13px; margin-bottom: 10px; }}
  th, td {{ border:1px solid #30363d; padding:6px 10px; text-align:right; }}
  th {{ background:#161b22; color:#8b949e; }}
  td:first-child, th:first-child {{ text-align:left; }}
  .pos {{ color:#7ee787; }}
  .neg {{ color:#ff7b72; }}
  .note {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px 16px; font-size:13px; color:#c9d1d9; margin-top:16px; line-height:1.6; }}
  .log {{ background:#0d1117; border:1px solid #30363d; border-radius:8px; padding:12px; font-size:12px; font-family:monospace; max-height:400px; overflow-y:auto; white-space:pre-wrap; }}
  a {{ color:#58a6ff; }}
</style>
</head>
<body>
  <h1>📊 Backtest SBR / RBS / QMS / QMR</h1>
  {status_html}

  <div class="cards">
    <div class="card"><div class="label">Total Trade</div><div class="value">{cr['n_trades']}</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value">{cr['wr']:.1f}%</div></div>
    <div class="card"><div class="label">Total R</div><div class="value {'pos' if cr['total_r']>=0 else 'neg'}">{cr['total_r']:+.2f}</div></div>
    <div class="card"><div class="label">Avg R/Trade</div><div class="value {'pos' if cr['avg_r']>=0 else 'neg'}">{cr['avg_r']:+.2f}</div></div>
    <div class="card"><div class="label">Balance Akhir</div><div class="value">${cr['final_balance']:.2f}</div></div>
    <div class="card"><div class="label">ROI</div><div class="value {'pos' if cr['roi']>=0 else 'neg'}">{cr['roi']:+.1f}%</div></div>
  </div>

  <div class="note">
    💡 <b>4 jenis level, semua basis body candle H1</b> (level dasar disyaratkan candle
    kiri MAUPUN kanan tidak menembus body-nya):
    <br>• <b>SBR</b> (Support→Resistance): support di-TEST (wick bawah sentuh, close aman) →
    BREAK ke bawah (close tembus) → KONFIRMASI (wick atas tidak balik ke level DAN body candle
    bearish, searah break). Entry <b>Short</b>.
    <br>• <b>RBS</b> (Resistance→Support): resistance di-TEST (wick atas sentuh, close aman) →
    BREAK ke atas → KONFIRMASI (wick bawah tidak balik ke level DAN body candle bullish, searah
    break). Entry <b>Long</b>.
    <br>• <b>QMS</b> (Quasimodo Support): support di-TEST → BREAK-1 ke bawah → BREAK-2 candle
    berikutnya balik ke ATAS di level yang sama → KONFIRMASI (low tidak menyentuh level DAN
    body candle bullish, searah break-2). Entry <b>Long</b> di level support awal.
    <br>• <b>QMR</b> (Quasimodo Resistance): kebalikan QMS — resistance di-TEST → BREAK-1 ke
    atas → BREAK-2 balik ke BAWAH → KONFIRMASI (high tidak menyentuh level DAN body candle
    bearish, searah break-2). Entry <b>Short</b>.
    <br>Level aktif dipantau via candle M5: masuk radius <b>{APPROACH_PCT*100:.1f}%</b> dari
    level → limit dipasang persis di level; kalau menjauh lagi &gt;{APPROACH_PCT*100:.1f}%
    sebelum fill → limit dicabut (level tetap hidup, bisa coba lagi). SL fix
    <b>{SL_PCT*100:.2f}%</b> dari entry (=1R). <b>Trailing stop</b>: aktif begitu profit
    capai <b>{TRAIL_ACTIVATE_R:.1f}R</b>, lalu SL mengikuti <b>{TRAIL_STOP_R:.1f}R</b> di
    belakang harga tertinggi/terendah yang pernah dicapai (dipantau M5). Level MATI setelah 1x
    terisi (menang/kalah).
    <br>⚙️ Risk {RISK_PCT*100:.0f}% dari balance (compounding). Slot maksimum: {_fmt_max_concurrent()}.
    Sinyal terblokir — slot: {cr.get('blocked_by_slot',0)}, margin: {cr.get('blocked_by_margin',0)},
    min order: {cr.get('blocked_by_min_order',0)}.
    <br>Unduh semua trade: <a href="/trades.csv">/trades.csv</a> &nbsp;|&nbsp;
    Log mentah: <a href="/logs">/logs</a>
  </div>

  <h2>Ringkasan per Jenis Level</h2>
  <table>
    <tr><th>Jenis</th><th>N Trade</th><th>Win</th><th>WR%</th><th>Total R</th><th>Total PnL</th></tr>
    {kind_rows_html}
  </table>

  <h2>Ringkasan per Koin</h2>
  <table>
    <tr><th>Symbol</th><th>N Trade</th><th>Win</th><th>WR%</th><th>Total R</th><th>Total PnL</th></tr>
    {rows_html}
  </table>

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
        'symbol', 'kind', 'direction', 'entry', 'sl', 'exit', 'reason', 'r_mult',
        'pnl_usd', 'entry_ts', 'exit_ts', 'balance_after', 'level'],
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
