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
INITIAL_BALANCE  = float(os.environ.get('INITIAL_BALANCE', '30.0'))   # modal awal PER COIN
RISK_PCT         = float(os.environ.get('RISK_PCT', '0.01'))          # risk 1% balance/trade (compound)
FEE_PCT          = float(os.environ.get('FEE_PCT', '0.00055'))        # taker fee per sisi (Bybit USDT perp)
EMA_FAST         = int(os.environ.get('EMA_FAST', '4'))
EMA_SLOW         = int(os.environ.get('EMA_SLOW', '10'))
TRAIL_ACT_R      = float(os.environ.get('TRAIL_ACT_R', '6.0'))        # trailing aktif di rasio 1:6
TRAIL_STOP       = float(os.environ.get('TRAIL_STOP', '1.0'))         # lebar trailing = 1x dist
MIN_DIST_PCT     = float(os.environ.get('MIN_DIST_PCT', '0.002'))     # floor SL minimum 0.2%
BACKTEST_DAYS    = int(os.environ.get('BACKTEST_DAYS', '300'))        # rentang histori H1 yang di-backtest

SYMBOLS = [
    'XPLUSDT', 'MNTUSDT', 'PLUMEUSDT', 'HYPEUSDT', 'BNBUSDT', 'BELUSDT', 'BERAUSDT', 'DASHUSDT',
    'DOGEUSDT', 'USUALUSDT', 'TAOUSDT', 'ESPORTSUSDT', 'LABUSDT', 'HUSDT', 'AVAXUSDT', 'REUSDT',
    '1000BONKUSDT', 'ORCAUSDT', 'AAVEUSDT', 'GMXUSDT', 'LTCUSDT', 'ICPUSDT', 'VIRTUALUSDT', 'CFXUSDT',
    'UNIUSDT', 'ONDOUSDT', 'SUIUSDT', 'ALGOUSDT', 'HBARUSDT', 'EIGENUSDT', 'XRPUSDT', 'SOLUSDT',
    'CRVUSDT', 'RENDERUSDT', 'XVGUSDT', 'SANDUSDT', 'AXSUSDT', 'IMXUSDT', 'FARTCOINUSDT', 'OPUSDT',
    '1000PEPEUSDT', 'TIAUSDT', 'GALAUSDT', 'APEUSDT', 'FLOWUSDT',
]

_END_MS   = int(time.time() * 1000)
_START_MS = _END_MS - BACKTEST_DAYS * 86400 * 1000

# ============================================================
# GLOBAL STATE (dibaca oleh HTTP handler, ditulis oleh background thread)
# ============================================================
_lock       = threading.Lock()
_log        = []
_phase      = 'running'     # running | done | error
_results    = []            # list per-coin dict
_all_trades = []            # semua trade, semua coin (utk CSV & agregat)


def _ts():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime('%H:%M:%S')

def _log_msg(msg: str):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    with _lock:
        _log.append(line)


# ============================================================
# FETCH DATA H1 DARI BYBIT
# ============================================================

def fetch_bybit_h1(symbol: str) -> pd.DataFrame:
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
# BACKTEST ENGINE per coin (EMA cross reversal + wick entry + flip protection)
# ============================================================

def backtest_coin(symbol, df):
    n = len(df)
    warmup = EMA_SLOW + 5
    if n < warmup + 10:
        return {'symbol': symbol, 'status': 'skip', 'reason': 'data kurang', 'trades': []}

    O, H, L, C = df['open'].values, df['high'].values, df['low'].values, df['close'].values
    TS = df['ts'].values
    ema_fast = df['close'].ewm(span=EMA_FAST, adjust=False).mean().values
    ema_slow = df['close'].ewm(span=EMA_SLOW, adjust=False).mean().values
    events = find_sr_events(df)
    events_by_c3 = {}
    for e in events:
        events_by_c3.setdefault(e['c3'], []).append(e)

    armed   = {'Short': None, 'Long': None}
    pending = {'Short': None, 'Long': None}
    active  = {'Short': None, 'Long': None}
    trades  = []
    balance = INITIAL_BALANCE

    def close_trade(direction, exit_price, reason, i):
        nonlocal balance
        pos = active[direction]
        entry, dist, qty = pos['entry'], pos['dist'], pos['qty']
        pnl_gross = (exit_price - entry) * qty if direction == 'Long' else (entry - exit_price) * qty
        fee = (entry * qty + exit_price * qty) * FEE_PCT
        pnl_net = pnl_gross - fee
        balance += pnl_net
        r_mult = pnl_gross / (dist * qty) if dist * qty else 0
        trades.append({
            'symbol': symbol, 'direction': direction, 'entry': entry, 'sl': pos['sl'],
            'exit': exit_price, 'reason': reason, 'r_mult': r_mult, 'pnl_usd': pnl_net,
            'entry_ts': pos['entry_ts'], 'exit_ts': int(TS[i]), 'balance_after': balance,
        })
        active[direction] = None

    for i in range(warmup, n - 1):
        death_cross  = ema_fast[i-1] >= ema_slow[i-1] and ema_fast[i] < ema_slow[i]
        golden_cross = ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]

        # 1) FLIP PROTECTION
        if death_cross and active['Long'] is not None:
            close_trade('Long', O[i+1], 'FLIP', i)
        if death_cross:
            pending['Long'] = None
        if golden_cross and active['Short'] is not None:
            close_trade('Short', O[i+1], 'FLIP', i)
        if golden_cross:
            pending['Short'] = None

        # 2) SL / trailing normal
        for direction in ('Short', 'Long'):
            pos = active[direction]
            if pos is None:
                continue
            h, l = H[i], L[i]
            if direction == 'Long':
                if l <= pos['stop']:
                    reason = 'TRAIL' if pos['trail_active'] else 'SL'
                    close_trade('Long', pos['stop'], reason, i)
                    continue
                pos['peak'] = max(pos['peak'], h)
                if not pos['trail_active'] and pos['peak'] >= pos['act_price']:
                    pos['trail_active'] = True
                if pos['trail_active']:
                    pos['stop'] = max(pos['stop'], pos['peak'] - TRAIL_STOP * pos['dist'])
            else:
                if h >= pos['stop']:
                    reason = 'TRAIL' if pos['trail_active'] else 'SL'
                    close_trade('Short', pos['stop'], reason, i)
                    continue
                pos['peak'] = min(pos['peak'], l)
                if not pos['trail_active'] and pos['peak'] <= pos['act_price']:
                    pos['trail_active'] = True
                if pos['trail_active']:
                    pos['stop'] = min(pos['stop'], pos['peak'] + TRAIL_STOP * pos['dist'])

        # 3) cek fill pending (limit di wick)
        for direction in ('Short', 'Long'):
            p = pending[direction]
            if p is not None and active[direction] is None:
                filled = (H[i] >= p['entry']) if direction == 'Short' else (L[i] <= p['entry'])
                if filled:
                    dist = p['dist']
                    min_dist = p['entry'] * MIN_DIST_PCT
                    if dist < min_dist:
                        dist = min_dist
                    risk_usd = balance * RISK_PCT
                    qty = risk_usd / dist if dist > 0 else 0
                    act_price = (p['entry'] + TRAIL_ACT_R * dist) if direction == 'Long' \
                                else (p['entry'] - TRAIL_ACT_R * dist)
                    active[direction] = {
                        'entry': p['entry'], 'sl': p['sl'], 'dist': dist, 'stop': p['sl'],
                        'trail_active': False, 'peak': p['entry'], 'act_price': act_price,
                        'qty': qty, 'entry_ts': int(TS[i]),
                    }
                    pending[direction] = None

        # 4) daftarkan support/resistance valid baru -> armed (bias arah, tetap hidup)
        for e in events_by_c3.get(i, []):
            if not e['valid']:
                continue
            if e['type'] == 'support':
                armed['Short'] = {'c1_ts': e['c1_ts']}
            else:
                armed['Long'] = {'c1_ts': e['c1_ts']}

        # 5) cross SEARAH -> pasang/ganti limit di wick
        if death_cross and armed['Short'] is not None and active['Short'] is None:
            wick = H[i]; old_dist = wick - C[i]
            if old_dist > 0:
                pending['Short'] = {'entry': wick, 'sl': wick + old_dist, 'dist': old_dist}
        if golden_cross and armed['Long'] is not None and active['Long'] is None:
            wick = L[i]; old_dist = C[i] - wick
            if old_dist > 0:
                pending['Long'] = {'entry': wick, 'sl': wick - old_dist, 'dist': old_dist}

    wins  = [t for t in trades if t['r_mult'] > 0]
    total_pnl = sum(t['pnl_usd'] for t in trades)
    return {
        'symbol': symbol, 'status': 'ok', 'trades': trades,
        'n_trades': len(trades), 'n_win': len(wins), 'n_loss': len(trades) - len(wins),
        'wr': (len(wins) / len(trades) * 100) if trades else 0,
        'total_pnl': total_pnl, 'final_balance': balance,
        'roi': (total_pnl / INITIAL_BALANCE * 100) if INITIAL_BALANCE else 0,
        'total_r': sum(t['r_mult'] for t in trades),
        'avg_r': (sum(t['r_mult'] for t in trades) / len(trades)) if trades else 0,
    }


# ============================================================
# BACKGROUND RUNNER
# ============================================================

def _run():
    global _phase
    _log_msg(f"🚀 Mulai backtest {len(SYMBOLS)} coin | H1 | {BACKTEST_DAYS} hari terakhir | "
              f"EMA {EMA_FAST}/{EMA_SLOW} | Trail 1:{TRAIL_ACT_R:.0f} | Risk {RISK_PCT*100:.0f}%/trade")
    for sym in SYMBOLS:
        try:
            _log_msg(f"📥 {sym}: mengambil data H1 dari Bybit...")
            df = fetch_bybit_h1(sym)
            if df.empty:
                _log_msg(f"   ⚠ {sym}: data kosong, skip.")
                with _lock:
                    _results.append({'symbol': sym, 'status': 'skip', 'reason': 'no data', 'trades': []})
                continue
            _log_msg(f"   {len(df):,} candle H1 diperoleh. Menjalankan backtest...")
            result = backtest_coin(sym, df)
            with _lock:
                _results.append(result)
                _all_trades.extend(result.get('trades', []))
            if result['status'] == 'ok':
                _log_msg(f"   ✅ {sym}: {result['n_trades']} trade | WR {result['wr']:.1f}% | "
                          f"PnL ${result['total_pnl']:+.2f} | ROI {result['roi']:+.1f}%")
            else:
                _log_msg(f"   ⚠ {sym}: {result.get('reason','skip')}")
        except Exception as e:
            _log_msg(f"   ❌ {sym}: error — {e}")
            with _lock:
                _results.append({'symbol': sym, 'status': 'error', 'reason': str(e), 'trades': []})
    with _lock:
        _phase = 'done'
    _log_msg("🏁 Backtest semua coin selesai.")


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

    refresh = '<meta http-equiv="refresh" content="5">' if phase == 'running' else ''
    chip_cls = {'running': 'running', 'done': 'done', 'error': 'error'}.get(phase, 'running')
    chip_txt = {'running': '⏳ Sedang berjalan...', 'done': '✅ Selesai', 'error': '❌ Error'}.get(phase, phase)

    ok_results = [r for r in res_cp if r.get('status') == 'ok']
    n_done, n_total = len(res_cp), len(SYMBOLS)

    # ── ringkasan gabungan ──
    total_trades = sum(r['n_trades'] for r in ok_results)
    total_win    = sum(r['n_win'] for r in ok_results)
    total_pnl    = sum(r['total_pnl'] for r in ok_results)
    total_r      = sum(r['total_r'] for r in ok_results)
    wr_overall   = (total_win / total_trades * 100) if total_trades else 0
    avg_r        = (total_r / total_trades) if total_trades else 0
    invested     = INITIAL_BALANCE * len(SYMBOLS)
    roi_overall  = (total_pnl / invested * 100) if invested else 0

    gross_win  = sum(t['pnl_usd'] for t in trades_cp if t['pnl_usd'] > 0)
    gross_loss = abs(sum(t['pnl_usd'] for t in trades_cp if t['pnl_usd'] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 0 else float('inf')

    summary_html = f'''
    <h2>Ringkasan Gabungan ({n_done}/{n_total} coin selesai)</h2>
    <table>
      <tr><th>Total Trade</th><th>Win</th><th>Loss</th><th>WR%</th><th>Total PnL</th>
          <th>ROI%</th><th>Avg R/trade</th><th>Profit Factor</th></tr>
      <tr>
        <td>{total_trades}</td>
        <td class="g">{total_win}</td>
        <td class="r">{total_trades-total_win}</td>
        <td class="{'g' if wr_overall>=40 else ('y' if wr_overall>=25 else 'r')}">{wr_overall:.1f}%</td>
        <td class="{'g' if total_pnl>=0 else 'r'}">${total_pnl:+.2f}</td>
        <td class="{'g' if roi_overall>=0 else 'r'}">{roi_overall:+.1f}%</td>
        <td class="{'g' if avg_r>=0 else 'r'}">{avg_r:+.3f}</td>
        <td>{pf:.2f}</td>
      </tr>
    </table>
    '''

    # ── tabel per coin ──
    rows = ''
    for r in sorted(res_cp, key=lambda x: -(x.get('total_pnl', -1e18) if x.get('status')=='ok' else -1e18)):
        if r.get('status') != 'ok':
            rows += (f'<tr><td>{r["symbol"]}</td><td colspan="7" class="y">'
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
            f'<td class="{pnl_c}">{r["roi"]:+.1f}%</td>'
            f'<td>{r["avg_r"]:+.3f}</td></tr>\n'
        )
    coin_table = f'''
    <table>
      <tr><th>Coin</th><th>Trade</th><th>Win</th><th>Loss</th><th>WR%</th>
          <th>PnL$</th><th>ROI%</th><th>Avg R</th></tr>
      {rows or '<tr><td colspan="8" class="y">Menunggu hasil...</td></tr>'}
    </table>
    '''

    log_html = '\n'.join(log_cp[-400:])

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
    Modal: <b>${INITIAL_BALANCE:.0f}/coin</b> &nbsp;|&nbsp;
    Rentang: <b>{BACKTEST_DAYS} hari terakhir</b> &nbsp;|&nbsp;
    EMA <b>{EMA_FAST}/{EMA_SLOW}</b> &nbsp;|&nbsp;
    Trail aktif <b>1:{TRAIL_ACT_R:.0f}</b> &nbsp;|&nbsp;
    Status: <span class="chip {chip_cls}">{chip_txt}</span>
  </p>

  {summary_html}

  <h2>Hasil Per Coin</h2>
  <div class="tbl-wrap">{coin_table}</div>

  <div class="note">
    💡 Entry = LIMIT di wick candle penyebab EMA cross. SL = wick diperpanjang sejauh
    jarak yang sama. Support valid → bias Short, Resistance valid → bias Long (arah dibalik).
    Flip protection: cross berlawanan → keluar/batal seketika, tunggu cross searah lagi.
    Trailing aktif di rasio 1:{TRAIL_ACT_R:.0f}, lebar {TRAIL_STOP:.1f}x dist.
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
        'pnl_usd', 'entry_ts', 'exit_ts', 'balance_after'])
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
