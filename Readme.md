# EMA-Cross Reversal Bot (Support/Resistance H1 + Flip Protection)

Repo ini berisi **2 aplikasi terpisah**:

| File | Fungsi | Butuh API Key? |
|---|---|---|
| `backtest_web.py` | Backtest semua koin, hasil (WR/PnL/ROI) bisa dilihat di website | Tidak (data publik) |
| `bot_ema_flip.py` | Bot live trading beneran di Bybit | Ya |

Default `Procfile`/`railway.toml` di repo ini menjalankan **`backtest_web.py`** (dashboard backtest). Kalau mau live trading, deploy `bot_ema_flip.py` sebagai service Railway yang terpisah (lihat bagian paling bawah).

## Strategi

Hasil riset & backtest paling optimal sejauh ini (XRPUSDT 10 bulan H1): **Total +249.45R, win rate 42.7%, avg +1.07R/trade** (murni R-multiple, belum termasuk fee & compounding).

1. **Deteksi support/resistance** (basis body candle H1):
   - Support: candle turun → candle naik → candle ketiga tidak boleh close/wick lebih rendah dari level support.
   - Resistance: kebalikannya.
   - Dianggap **valid** kalau wick pembentuknya menyentuh level S/R sejenis sebelumnya yang masih "hidup" (belum pernah ditembus close candle manapun).

2. **Arah dibalik**:
   - Support valid → bias **SHORT**.
   - Resistance valid → bias **LONG**.
   - Bias tetap "hidup" untuk re-entry berulang sampai muncul support/resistance valid yang benar-benar baru.

3. **Entry via EMA Cross** (EMA4 & EMA10, H1):
   - Bias Short + **death cross** → limit **SELL** di **wick (high)** candle penyebab cross. SL = wick + jarak yang sama ke arah berlawanan.
   - Bias Long + **golden cross** → limit **BUY** di **wick (low)** candle cross. SL = wick − jarak yang sama.
   - Cross searah baru sebelum limit lama fill → limit lama diganti ke wick terbaru.

4. **Flip Protection**:
   - Sedang pending atau sudah punya posisi di satu arah, lalu muncul cross **berlawanan** → limit dibatalkan / posisi ditutup market **saat itu juga**, tidak peduli profit atau rugi.
   - Bias tetap hidup, lanjut menunggu cross searah berikutnya.

5. **Trailing stop**: aktif otomatis setelah profit mencapai rasio **1:6** dari jarak entry-SL (`TRAIL_ACT_R`), lebar trailing 1× jarak (`TRAIL_STOP`).

---

## 1. Deploy Backtest Dashboard (default)

1. Push repo ini ke GitHub.
2. Buat project baru di [Railway](https://railway.app), connect ke repo.
3. Railway otomatis jalankan `python backtest_web.py` (dari `railway.toml`/`Procfile`).
4. **Tidak perlu isi API key apapun** — data candle diambil dari endpoint publik Bybit.
5. (Opsional) atur env var di tab Variables — lihat `.env.example` untuk daftar lengkap: `BACKTEST_DAYS`, `TRAIL_ACT_R`, `RISK_PCT`, `INITIAL_BALANCE`, dll.
6. Deploy. Buka domain Railway kamu di browser — dashboard akan menampilkan progress backtest tiap coin secara realtime (auto-refresh tiap 5 detik selama masih berjalan), lalu ringkasan Win Rate, Total PnL, ROI, Profit Factor, dan breakdown per-coin begitu selesai.
7. Endpoint tambahan:
   - `/trades.csv` — unduh semua trade hasil backtest (untuk analisis lebih lanjut)
   - `/logs` — log mentah proses backtest

**Catatan:** backtest butuh waktu (tergantung jumlah coin & `BACKTEST_DAYS`) karena fetch data H1 dari Bybit satu per satu. Untuk 44 coin × 300 hari, biasanya selesai dalam beberapa menit. Halaman dashboard bisa dibiarkan terbuka sambil menunggu (auto-refresh).

---

## 2. Deploy Bot Live Trading (opsional, service terpisah)

Kalau setelah lihat hasil backtest kamu mau lanjut live:

1. Di Railway, buat **service baru** (bukan menimpa yang backtest) dalam project yang sama, atau project baru, tetap connect ke repo yang sama.
2. Di service baru itu, override **Start Command** jadi:
   ```
   python bot_ema_flip.py
   ```
3. Isi env var wajib: `API_KEY`, `API_SECRET` (lihat bagian bawah `.env.example`).
4. `ALLOW_HEDGE=true` wajib — bot otomatis coba switch akun ke Hedge Mode saat start.
5. **Sangat disarankan tes di Testnet dulu** (`TESTNET=true`) sebelum live.
6. Endpoint sama seperti backtest: `/view` (log per koin), `/logs` (log mentah), `/ohlc` (unduh data candle yang dilihat bot).

## Environment Variables

Lihat `.env.example` untuk daftar lengkap dan penjelasan tiap variabel (dipisah antara bagian backtest dan bagian bot live).

## Menjalankan lokal (opsional)

```bash
pip install -r requirements.txt
cp .env.example .env
export $(cat .env | grep -v '^#' | xargs)   # linux/mac

python backtest_web.py     # jalankan dashboard backtest, lalu buka http://localhost:8080
# ATAU
python bot_ema_flip.py     # jalankan bot live (butuh API_KEY/API_SECRET)
```

## Peringatan

- Backtest historis **tidak menjamin** performa live — sudah termasuk simulasi fee taker Bybit (0.055%/sisi) tapi belum termasuk slippage realistis dan kondisi likuiditas order book aktual.
- Ini strategi agresif: win rate menengah (±40%), mengandalkan winner yang lari jauh via trailing untuk menutup banyak trade yang kena SL kecil.
- Selalu mulai live dengan `RISK_PCT` kecil dan `MAX_CONCURRENT` terbatas.
- Backtest awal dilakukan di 1 koin (XRPUSDT) — dashboard ini untuk memvalidasi ulang di **semua** koin sebelum live.
