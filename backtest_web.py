"""
backtest_snr.py — Backtest strategi Support & Resistance + TEST1/TEST2 (engulfing), H1
================================================================================
Level Support/Resistance dari 2 candle berlawanan arah (c1,c2), dikonfirmasi
1 candle kanan bersih (c3). Setelah itu MENUNGGU harga menyentuh "patokan"
(wick terpendek c1/c2), lalu cek candle berikutnya apakah ENGULFING searah.
Kalau ya -> entry MARKET. Tiap level HANYA dicoba 1x (tidak ada re-entry).

RINGKASAN STRATEGI
-------------------
1) DETEKSI LEVEL (H1):
   Support: candle c1 bearish (close<open) lalu c2 bullish (close>open).
            Level = close[c1].
   Resistance: candle c1 bullish (close>open) lalu c2 bearish (close<open).
            Level = close[c1].
   Kedua arah valid kalau:
            - KANAN: N_RIGHT candle SETELAH c1,c2 (default 2, c3 & c4) --
              WICK-nya tidak boleh menyentuh level SAMA SEKALI.
            - WICK: c1 DAN c2 WAJIB sama-sama punya wick sungguhan (beda
              dari body). TIDAK ADA lagi keharusan salah satu lebih panjang
              -- boleh acak, c1 atau c2 yang lebih panjang, bebas.
              TANPA syarat kiri sama sekali (candle sebelum c1 tidak dicek).
   'patokan' = ujung wick yang PALING PENDEK di antara c1/c2 (tip paling
              dekat ke level) -- dipakai sebagai acuan TEST1 di bawah.

2) TEST1 -- candle PERTAMA yang wick/body-nya MELEBIHI patokan (support:
   low < patokan; resistance: high > patokan; harus benar2 melebihi,
   menyentuh persis di harga patokan TIDAK dihitung), APAPUN arahnya.
   - Kalau candle breach pertama ini SEARAH arah trade (Support->bullish,
     Resistance->bearish) -> lanjut ke TEST2.
   - Kalau breach pertama ini SALAH ARAH -> level GUGUR PERMANEN saat itu
     juga -- TIDAK mencari candle breach berikutnya lagi.

3) TEST2 -- candle TEPAT SETELAH TEST1, harus:
   - ENGULFING SEARAH: Support (Long): ujung body TEST2 (max(open,close))
     harus LEBIH TINGGI dari HIGH candle TEST1 (ujung atas wick TEST1).
     Resistance (Short): ujung body TEST2 (min(open,close)) harus LEBIH
     RENDAH dari LOW candle TEST1 (ujung bawah wick TEST1).
   - DAN candle TEST2 juga SEARAH arah trade (sama seperti syarat TEST1).
   Kalau salah satu gagal -> level GUGUR (hanya dicoba SEKALI, tidak
   dicari TEST1 berikutnya lagi).

4) ENTRY -- MARKET, persis begitu candle TEST2 closed. entry_price = close
   candle TEST2. Arah: Support -> Long, Resistance -> Short.
   SL = SL_PCT (default 1% = 1R) dari harga entry, arah berlawanan dari entry.
   TRAILING STOP: begitu profit capai TRAIL_ACTIVATE_R (default 3R), trailing
   aktif -- SL lalu mengikuti TRAIL_STOP_R (default 1R) di belakang harga
   tertinggi/terendah yang pernah dicapai (dipantau M5), SL cuma boleh
   bergerak menguntungkan, tak pernah mundur.
   Level MATI (tidak dipakai lagi) setelah 1x FILLED (menang ataupun kalah).

Deploy ke Railway:
  Start command -> python backtest_snr.py
  Buka domain Railway -> lihat progress & hasil di browser (auto-refresh)

Periode default: 2 bulan ke belakang dari hari script dijalankan (bisa
di-override lewat env BACKTEST_START_DATE / BACKTEST_END_DATE).

trades.csv sekarang menyertakan waktu (WIB, UTC+7) level terbentuk,
waktu entry, dan waktu exit -- kolom 'level_formed_wib', 'entry_wib',
'exit_wib' -- selain versi epoch ms mentahnya. Kolom 'reason' berisi
'SL' atau 'TRAIL' (hasil exit karena stop-loss awal atau trailing stop).
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

LEVERAGE           = float(os.environ.get('LEVERAGE', '50'))
MARGIN_USAGE_CAP    = float(os.environ.get('MARGIN_USAGE_CAP', '0.90'))

MAX_CONCURRENT_RAW = os.environ.get('MAX_CONCURRENT', '0').strip().lower()
MAX_CONCURRENT = float('inf') if MAX_CONCURRENT_RAW in ('', '0', 'unlimited', 'inf') else int(MAX_CONCURRENT_RAW)

ALLOW_HEDGE      = os.environ.get('ALLOW_HEDGE', 'true').lower() == 'true'

SIMULATE_MIN_ORDER  = os.environ.get('SIMULATE_MIN_ORDER', '1').strip() not in ('0', 'false', 'False', '')
MIN_ORDER_USD       = float(os.environ.get('MIN_ORDER_USD', '5.0'))
ORDER_BUMP_FLOOR     = float(os.environ.get('ORDER_BUMP_FLOOR', '4.0'))
QTY_STEP_APPROX      = float(os.environ.get('QTY_STEP_APPROX', '0.000001'))

def _default_end_date():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def _default_start_date():
    d = datetime.now(timezone.utc)
    # mundur 1 bulan kalender (30 hari) dari hari ini
    return (d - timedelta(days=30)).strftime('%Y-%m-%d')

BACKTEST_START_DATE = os.environ.get('BACKTEST_START_DATE', _default_start_date())
BACKTEST_END_DATE   = os.environ.get('BACKTEST_END_DATE', _default_end_date())

CACHE_DIR = os.environ.get('CACHE_DIR', './data_cache')
os.makedirs(CACHE_DIR, exist_ok=True)

# Semua koin yang dipakai bot.
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
_per_coin_results = []   # list dict: {symbol, n_trades, n_win, wr, total_r, final_balance, roi}
_all_trades = []
_combined_result = {
    'n_trades': 0, 'n_win': 0, 'n_loss': 0, 'wr': 0, 'total_pnl': 0, 'roi': 0,
    'total_r': 0, 'avg_r': 0, 'final_balance': INITIAL_BALANCE,
    'blocked_by_slot': 0, 'blocked_by_margin': 0, 'blocked_by_min_order': 0, 'blocked_by_invalid_sl': 0,
}


def _ts():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%H:%M:%S')

def _fmt_wib(ts_ms):
    """epoch ms (UTC) -> 'YYYY-MM-DD HH:MM WIB' (UTC+7)."""
    if ts_ms is None:
        return ''
    dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc) + timedelta(hours=7)
    return dt.strftime('%Y-%m-%d %H:%M WIB')

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

N_RIGHT = 2   # jumlah candle kanan yang harus bersih (tidak menyentuh wick) -- c3 dan c4

def find_levels(df):
    """Deteksi level Support & Resistance dari candle H1 (basis body candle).
    TANPA syarat kiri lagi -- cukup c3 & c4 (2 candle tepat setelah c1,c2)
    yang wick-nya tidak boleh menyentuh level.
    Syarat kanan: N_RIGHT candle SETELAH c1,c2 (c3, c4) -- WICK-nya (bukan cuma
                  body) tidak boleh menyentuh level sama sekali.
    Syarat wick c1 vs c2 (RANDOM, tidak ada urutan lagi):
      - c1 WAJIB punya wick sungguhan -- ujung wick c1 tidak boleh sama
        persis dengan ujung body c1 (mis. ujung body 0.1, ujung wick harus
        beda, misal 0.09 -- bukan sama-sama 0.1). Kalau sama (tidak ada
        wick), level GUGUR.
      - c2 JUGA wajib punya wick sungguhan (syarat sama seperti c1).
      - Tidak ada lagi keharusan salah satu lebih panjang dari yang lain --
        boleh c1 lebih panjang dari c2, atau sebaliknya.
    'patokan' = ujung wick yang PALING PENDEK di antara c1 dan c2 (yang
                  tipnya PALING DEKAT ke level) -- dipakai sebagai acuan
                  test1/test2 (lihat detect_snr_events).
                  Support -> max(low[c1], low[c2]). Resistance -> min(high[c1], high[c2]).
    'level' tetap = body candle c1 (close[c1]) -- dipakai untuk syarat
                  validitas candle c3 (wick tidak boleh menyentuh level ini).
    Return list dict: {'type', 'level', 'patokan', 'c1', 'c2', 'c_right'}."""
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    n = len(df)
    levels = []
    WICK_EPS = 1e-9   # beda minimum supaya wick dianggap "ada" (tidak sama dgn body)
    for i in range(0, n - (1 + N_RIGHT)):   # cuma perlu c1,c2 + N_RIGHT candle kanan (c3)
        if c[i] < o[i] and c[i + 1] > o[i + 1]:          # bearish lalu bullish -> support
            S = c[i]
            right_ok = all(l[i + 2 + k] > S + 1e-9 for k in range(N_RIGHT))
            lower_wick_c1 = c[i] - l[i]           # wick bawah c1 (bearish): close - low
            lower_wick_c2 = o[i + 1] - l[i + 1]   # wick bawah c2 (bullish): open - low
            wick_ok = (lower_wick_c1 > WICK_EPS) and (lower_wick_c2 > WICK_EPS)
            if right_ok and wick_ok:
                patokan = max(l[i], l[i + 1])   # wick TERPENDEK (tip paling dekat ke level)
                levels.append({'type': 'support', 'level': S, 'patokan': patokan,
                                'c1': i, 'c2': i + 1, 'c_right': [i + 2 + k for k in range(N_RIGHT)]})
        if c[i] > o[i] and c[i + 1] < o[i + 1]:          # bullish lalu bearish -> resistance
            R = c[i]
            right_ok = all(h[i + 2 + k] < R - 1e-9 for k in range(N_RIGHT))
            upper_wick_c1 = h[i] - c[i]          # wick atas c1 (bullish): high - close
            upper_wick_c2 = h[i + 1] - o[i + 1]  # wick atas c2 (bearish): high - open
            wick_ok = (upper_wick_c1 > WICK_EPS) and (upper_wick_c2 > WICK_EPS)
            if right_ok and wick_ok:
                patokan = min(h[i], h[i + 1])   # wick TERPENDEK (tip paling dekat ke level)
                levels.append({'type': 'resistance', 'level': R, 'patokan': patokan,
                                'c1': i, 'c2': i + 1, 'c_right': [i + 2 + k for k in range(N_RIGHT)]})
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

def detect_snr_events(df):
    """Deteksi level Support & Resistance, lalu cari TEST1+TEST2 (engulfing)
    utk tiap level yang terbentuk. Entry = MARKET, setelah engulfing terjadi.

    Urutan:
    1) Level terbentuk (find_levels): c1+c2 (2 candle berlawanan arah), c3+c4
       bersih (wick tidak menyentuh level). 'patokan' = ujung wick TERPENDEK
       di antara c1/c2 (tip paling dekat ke level).
    2) TEST1: mulai scan dari candle SETELAH c3/c4, cari candle PERTAMA yang
       wick/body-nya MELEBIHI patokan (support: low < patokan; resistance:
       high > patokan) -- harus benar2 melebihi, bukan cuma sama persis
       dengan harga patokan. APAPUN arahnya candle ini:
       - SEARAH dengan arah trade (Support->bullish, Resistance->bearish)
         -> lanjut ke TEST2.
       - SALAH ARAH -> level GUGUR PERMANEN saat itu juga, TIDAK mencari
         candle breach berikutnya lagi.
    3) TEST2: candle TEPAT SETELAH candle TEST1 -- harus:
       - ENGULFING SEARAH:
         Support (Long): ujung body candle TEST2 (max(open,close)) harus
         LEBIH TINGGI dari ujung ATAS wick candle TEST1 (high candle TEST1).
         Resistance (Short): ujung body candle TEST2 (min(open,close)) harus
         LEBIH RENDAH dari ujung BAWAH wick candle TEST1 (low candle TEST1).
       - DAN candle TEST2 juga SEARAH dengan arah trade (sama seperti syarat
         TEST1: Support -> bullish, Resistance -> bearish).
       Kalau salah satu syarat TEST2 gagal -> level GUGUR (hanya dicoba
       SEKALI, tidak dicari TEST1 berikutnya lagi).
    4) ENTRY: MARKET, begitu candle TEST2 closed. entry_price = close candle
       TEST2, entry_ts = waktu (ts) candle TEST2.

    Return list dict:
    {'kind': 'SNR_SUPPORT'/'SNR_RESISTANCE', 'type': support/resistance,
     'level': harga body c1, 'patokan', 'direction': Long/Short,
     'entry_price', 'entry_ts', 'test1_ts', 'confirm_ts', 'c1_ts', 'c1', 'c2'}
    """
    o = df['open'].values; h = df['high'].values; l = df['low'].values; c = df['close'].values
    ts = df['ts'].values
    n = len(df)
    WICK_EPS = 1e-9
    levels = find_levels(df)
    events = []

    for lv in levels:
        level = lv['level']
        ty = lv['type']
        patokan = lv['patokan']
        c1 = lv['c1']
        last_right_i = lv['c_right'][-1]   # c4 -- confirm

        # TEST1: candle PERTAMA yang MELEBIHI patokan (breach), apapun arahnya.
        # Kalau breach pertama ini SEARAH trade -> lanjut ke TEST2.
        # Kalau breach pertama ini SALAH ARAH -> level GUGUR PERMANEN, tidak
        # lanjut mencari candle breach berikutnya.
        test1_i = None
        for k in range(last_right_i + 1, n - 1):   # -1: butuh k+1 (TEST2) tersedia
            if ty == 'support':
                breach = l[k] < patokan - WICK_EPS
            else:
                breach = h[k] > patokan + WICK_EPS
            if breach:
                test1_i = k
                break
        if test1_i is None:
            continue   # belum pernah tersentuh sampai akhir data -> tidak ada sinyal

        if ty == 'support':
            same_dir = c[test1_i] > o[test1_i]   # bullish
        else:
            same_dir = c[test1_i] < o[test1_i]   # bearish
        if not same_dir:
            continue   # breach pertama SALAH ARAH -> level gugur permanen

        t2 = test1_i + 1
        if ty == 'support':
            body_top_t2 = max(o[t2], c[t2])
            engulf_ok = body_top_t2 > h[test1_i] + WICK_EPS
            t2_same_dir = c[t2] > o[t2]   # bullish
        else:
            body_bottom_t2 = min(o[t2], c[t2])
            engulf_ok = body_bottom_t2 < l[test1_i] - WICK_EPS
            t2_same_dir = c[t2] < o[t2]   # bearish
        if not (engulf_ok and t2_same_dir):
            continue   # TEST2 gagal (bukan engulfing dan/atau bukan searah) -> level gugur

        kind = 'SNR_SUPPORT' if ty == 'support' else 'SNR_RESISTANCE'
        direction = 'Long' if ty == 'support' else 'Short'
        events.append({
            'kind': kind, 'type': ty, 'level': level, 'patokan': patokan,
            'direction': direction,
            'entry_price': float(c[t2]), 'entry_ts': int(ts[t2]),
            'test1_ts': int(ts[test1_i]),
            'confirm_ts': int(ts[last_right_i]),
            'c1_ts': int(ts[c1]),
            'c1': lv['c1'], 'c2': lv['c2'],
        })

    events.sort(key=lambda e: e['entry_ts'])
    return events


def detect_all_events(df):
    """Support & Resistance dgn TEST1+TEST2 (engulfing) -- SNR_SUPPORT (Long)
    dan SNR_RESISTANCE (Short). Dedup: 2 event dgn (kind, level, entry_ts)
    SAMA PERSIS dianggap 1 sinyal yg sama -- ambil salah satu saja."""
    events = detect_snr_events(df)
    seen = set()
    deduped = []
    for e in events:
        dedup_key = (e['kind'], round(e['level'], 10), e['entry_ts'])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        deduped.append(e)
    deduped.sort(key=lambda e: e['entry_ts'])
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
# Tiap event dari detect_all_events() SUDAH final (test1+test2/engulfing
# sudah lolos di tahap deteksi) -- tinggal 1 aksi: begitu waktu (M5) sampai
# di entry_ts event itu, coba MARKET ENTRY sekali (kalau slot/margin/min
# order tidak cukup PAS di momen itu, event ini gugur, tidak dicoba ulang).

def run_combined_backtest(coins: dict, m5_data: dict) -> dict:
    balance = INITIAL_BALANCE
    active_positions = {}     # key(symbol,direction+level) -> {...}
    trades = []

    def _akey(symbol, direction, level, kind):
        return f"{symbol}|{direction}|{kind}|{level:.10f}"

    total_margin_used = 0.0   # dijaga incremental, bukan sum() ulang tiap panggilan (O(1) bukan O(n))

    def _slots_used():
        return len(active_positions)

    positions_by_symbol = {}   # symbol -> set(keys)

    # pending_activation: event yg entry_ts-nya BELUM lewat, urut asc per simbol
    pending_activation = {}
    for symbol, cp in coins.items():
        pending_activation[symbol] = sorted(
            [(ev['entry_ts'], idx) for idx, ev in enumerate(cp['events'])])

    def open_trade(symbol, ev, entry_price, entry_ts):
        nonlocal balance, total_margin_used
        direction = ev['direction']
        if direction == 'Short':
            sl = entry_price * (1 + SL_PCT)
        else:
            sl = entry_price * (1 - SL_PCT)
        dist = abs(entry_price - sl)   # = 1R (fix, SL_PCT)

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
            'kind': ev['kind'], 'margin': margin_needed, 'confirm_ts': ev['confirm_ts'],
            'c1_ts': ev['c1_ts'], 'test1_ts': ev['test1_ts'],
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
            'level_formed_ts': pos['c1_ts'],
            'level_formed_wib': _fmt_wib(pos['c1_ts']),
            'test1_ts': pos['test1_ts'],
            'test1_wib': _fmt_wib(pos['test1_ts']),
            'entry_wib': _fmt_wib(pos['entry_ts']),
            'exit_wib': _fmt_wib(exit_ts),
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

    nonlocal_blocks = {'slot': 0, 'margin': 0, 'min_order': 0, 'invalid_sl': 0}

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
        first_entry_ts = cp['events'][0]['entry_ts']
        _advance_ptr_to(symbol, first_entry_ts + 1)
        if ptr[symbol] < m5_len[symbol]:
            heapq.heappush(heap, (int(m5_ts_arr[symbol][ptr[symbol]]), symbol))

    while heap:
        now_ts, symbol = heapq.heappop(heap)
        i = ptr[symbol]
        if i >= m5_len[symbol] or m5_ts_arr[symbol][i] != now_ts:
            continue   # stale entry (seharusnya tidak terjadi, safety check)

        # begitu waktu (M5) sudah lewat entry_ts sebuah event -> MARKET ENTRY
        # sekali, langsung (tidak ada lagi waiting/armed/approach).
        cp = coins[symbol]
        plist = pending_activation.get(symbol)
        if plist:
            while plist and plist[0][0] < now_ts:
                _, idx = plist.pop(0)
                ev = cp['events'][idx]
                opened_key, block_reason = open_trade(symbol, ev, ev['entry_price'], ev['entry_ts'])
                if opened_key is None:
                    nonlocal_blocks[block_reason] += 1

        process_symbol_tick(symbol, i)

        # advance pointer: kalau simbol masih punya posisi aktif (butuh dipantau
        # tiap candle utk SL/trailing), lanjut candle BERIKUTNYA (i+1, O(1)).
        # Kalau tidak ada posisi terbuka, lompat jauh ke entry_ts event pending
        # berikutnya (hemat banyak tick).
        has_active = bool(positions_by_symbol.get(symbol))
        if has_active:
            ptr[symbol] = i + 1
        else:
            plist = pending_activation.get(symbol)
            if plist:
                _advance_ptr_to(symbol, plist[0][0] + 1)
            else:
                ptr[symbol] = m5_len[symbol]   # tidak ada event pending lagi -> selesai

        if ptr[symbol] < m5_len[symbol]:
            heapq.heappush(heap, (int(m5_ts_arr[symbol][ptr[symbol]]), symbol))

    blocked_by_slot = nonlocal_blocks['slot']
    blocked_by_margin = nonlocal_blocks['margin']
    blocked_by_min_order = nonlocal_blocks['min_order']
    blocked_by_invalid_sl = nonlocal_blocks['invalid_sl']

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
        'blocked_by_min_order': blocked_by_min_order, 'blocked_by_invalid_sl': blocked_by_invalid_sl,
    }


# ============================================================
# SIMULASI PER-KOIN (independen, bukan gabungan)
# ============================================================
# Selain simulasi GABUNGAN (1 balance dipakai bersama semua koin -- lebih
# realistis krn simulasikan 1 akun beneran), kita juga jalankan simulasi
# PER-KOIN: tiap koin dapat modal awal sendiri (INITIAL_BALANCE), balance &
# slot terpisah dari koin lain. Ini menjawab pertanyaan "kalau CUMA trading
# koin ini sendirian, berapa trade & WR-nya" -- tanpa pengaruh rebutan
# margin/slot dari koin lain.

def run_per_coin_backtests(coins: dict, m5_data: dict) -> dict:
    """Return dict: symbol -> hasil run_combined_backtest (dgn 1 koin saja)."""
    results = {}
    for symbol, cp in coins.items():
        m5 = m5_data.get(symbol)
        if m5 is None:
            continue
        results[symbol] = run_combined_backtest({symbol: cp}, {symbol: m5})
    return results


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
    """Breakdown performa per JENIS level: SNR_SUPPORT / SNR_RESISTANCE."""
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
    order = {'SNR_SUPPORT': 0, 'SNR_RESISTANCE': 1}
    rows.sort(key=lambda r: order.get(r['kind'], 99))
    return rows


# ============================================================
# BACKGROUND WORKER
# ============================================================

def _run():
    global _phase, _results, _kind_results, _per_coin_results, _all_trades, _combined_result
    try:
        _log_msg(f"🚀 Mulai backtest SNR (Support & Resistance murni) — {len(SYMBOLS)} koin, {BACKTEST_START_DATE} s/d {BACKTEST_END_DATE}")
        _log_msg(f"   Entry=MARKET setelah TEST1 (patokan tersentuh) + TEST2 (engulfing)  "
                  f"SL={SL_PCT*100:.2f}% fix dari entry (=1R)  "
                  f"Trailing: aktif di "
                  f"{TRAIL_ACTIVATE_R:.1f}R, jarak {TRAIL_STOP_R:.1f}R dari extreme")

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

        _log_msg(f"🧮 Menjalankan simulasi PER-KOIN (independen, {len(coins)} koin)...")
        per_coin_raw = run_per_coin_backtests(coins, m5_data)
        per_coin_rows = []
        for symbol, r in per_coin_raw.items():
            per_coin_rows.append({
                'symbol': symbol, 'n_trades': r['n_trades'], 'n_win': r['n_win'],
                'wr': r['wr'], 'total_r': r['total_r'], 'final_balance': r['final_balance'],
                'roi': r['roi'],
            })
        per_coin_rows.sort(key=lambda r: -r['total_r'])

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
                'blocked_by_invalid_sl': result['blocked_by_invalid_sl'],
            })
            _results[:] = per_symbol_breakdown(result['trades'])
            _kind_results[:] = per_kind_breakdown(result['trades'])
            _per_coin_results[:] = per_coin_rows
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
        per_coin_cp = list(_per_coin_results)
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

    per_coin_rows_html = ''
    for r in per_coin_cp:
        cls = 'pos' if r['total_r'] >= 0 else 'neg'
        per_coin_rows_html += f'''<tr>
            <td>{r['symbol']}</td><td>{r['n_trades']}</td><td>{r['n_win']}</td>
            <td>{r['wr']:.1f}%</td><td class="{cls}">{r['total_r']:+.2f}</td>
            <td>${r['final_balance']:.2f}</td>
            <td class="{cls}">{r['roi']:+.1f}%</td></tr>'''

    return f'''<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<title>Backtest SNR</title>
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
  <h1>📊 Backtest SNR (Support & Resistance Murni)</h1>
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
    💡 <b>Support & Resistance + TEST1/TEST2 (engulfing)</b>, basis body candle H1:
    <br>• Level terbentuk dari c1+c2 (2 candle berlawanan arah). TANPA syarat kiri lagi --
    2 candle kanan (c3 & c4) yang wick-nya tidak boleh menyentuh level.
    <br>• Syarat wick: c1 DAN c2 wajib sama-sama punya wick sungguhan (beda dari body) --
    tidak ada lagi keharusan salah satu lebih panjang, boleh acak. <b>Patokan</b> = ujung wick
    yang PALING PENDEK di antara c1/c2 (tip paling dekat ke level).
    <br>• <b>TEST1</b>: candle PERTAMA yang wick/body-nya melebihi (bukan cuma menyentuh
    persis) patokan, apapun arahnya. Kalau breach pertama ini SEARAH trade (Support→bullish,
    Resistance→bearish) → lanjut ke TEST2. Kalau SALAH ARAH → level GUGUR PERMANEN saat itu
    juga (tidak mencari breach berikutnya).
    <br>• <b>TEST2</b>: candle TEPAT SETELAH TEST1 -- harus ENGULFING SEARAH (Support: ujung
    body TEST2 harus lebih TINGGI dari high candle TEST1. Resistance: ujung body TEST2 harus
    lebih RENDAH dari low candle TEST1) DAN candle-nya juga harus SEARAH trade. Kalau salah
    satu gagal, level gugur (hanya dicoba 1x).
    <br>• <b>ENTRY</b>: MARKET, persis begitu candle TEST2 closed (harga = close candle TEST2).
    Tiap level HANYA dipakai 1x (test1+test2 cuma dicoba sekali).
    SL fix
    <b>{SL_PCT*100:.2f}%</b> dari entry (=1R). <b>Trailing stop</b>: aktif begitu profit
    capai <b>{TRAIL_ACTIVATE_R:.1f}R</b>, lalu SL mengikuti <b>{TRAIL_STOP_R:.1f}R</b> di
    belakang harga tertinggi/terendah yang pernah dicapai (dipantau M5). Level MATI setelah
    1x terisi (menang/kalah).
    <br>⚙️ Risk {RISK_PCT*100:.0f}% dari balance (compounding). Slot maksimum: {_fmt_max_concurrent()}.
    Sinyal terblokir — slot: {cr.get('blocked_by_slot',0)}, margin: {cr.get('blocked_by_margin',0)},
    min order: {cr.get('blocked_by_min_order',0)}.
    <br>Unduh semua trade: <a href="/trades.csv">/trades.csv</a> &nbsp;|&nbsp;
    Log mentah: <a href="/logs">/logs</a>
  </div>

  <h2>Ringkasan per Jenis Level (simulasi gabungan)</h2>
  <table>
    <tr><th>Jenis</th><th>N Trade</th><th>Win</th><th>WR%</th><th>Total R</th><th>Total PnL</th></tr>
    {kind_rows_html}
  </table>

  <h2>Kontribusi per Koin (simulasi gabungan, 1 balance bersama)</h2>
  <table>
    <tr><th>Symbol</th><th>N Trade</th><th>Win</th><th>WR%</th><th>Total R</th><th>Total PnL</th></tr>
    {rows_html}
  </table>

  <h2>Simulasi Per-Koin (independen, modal awal sendiri-sendiri)</h2>
  <table>
    <tr><th>Symbol</th><th>N Trade</th><th>Win</th><th>WR%</th><th>Total R</th><th>Balance Akhir</th><th>ROI</th></tr>
    {per_coin_rows_html}
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
        'symbol', 'kind', 'direction', 'level',
        'level_formed_wib', 'test1_wib', 'entry_wib', 'exit_wib',
        'entry', 'sl', 'exit', 'reason', 'r_mult',
        'pnl_usd', 'balance_after',
        'level_formed_ts', 'test1_ts', 'entry_ts', 'exit_ts'],
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
