# Ingest of 1-minute klines from the official Binance public data (data.binance.vision) into a small lakehouse:
#   raw/     the monthly zip files as they come (bronze)
#   lake/    parquet partitioned by symbol, year and month, with clean types (silver)
#
# python ingest.py            (downloads only the months that are missing)

import io
import zipfile
from pathlib import Path

import polars as pl
import requests

RAIZ = Path(__file__).resolve().parent
SIMBOLOS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
MESES = [f"{a}-{m:02d}" for a in (2024, 2025, 2026) for m in range(1, 13) if f"{a}-{m:02d}" <= "2026-09"]
COLUMNAS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "trades",
            "taker_buy_volume", "taker_buy_quote_volume", "ignore"]

for simbolo in SIMBOLOS:
    for mes in MESES:
        anio, m = mes.split("-")
        destino = RAIZ / "lake" / "klines" / f"symbol={simbolo}" / f"year={anio}" / f"month={m}" / "part.parquet"
        if destino.exists():
            continue
        zip_path = RAIZ / "raw" / simbolo / f"{simbolo}-1m-{mes}.zip"
        if not zip_path.exists():
            url = f"https://data.binance.vision/data/spot/monthly/klines/{simbolo}/1m/{simbolo}-1m-{mes}.zip"
            r = requests.get(url, timeout=120)
            if r.status_code != 200:
                print("missing", simbolo, mes)
                continue
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            zip_path.write_bytes(r.content)
        z = zipfile.ZipFile(zip_path)
        df = pl.read_csv(io.BytesIO(z.read(z.namelist()[0])), has_header=False, new_columns=COLUMNAS)
        # Binance changed the timestamps of spot data from milliseconds to microseconds in January 2025
        unidad = "us" if df["open_time"][0] > 10 ** 14 else "ms"
        df = (df.with_columns(pl.from_epoch("open_time", time_unit=unidad).alias("ts"))
                .select(["ts", "open", "high", "low", "close", "volume", "quote_volume", "trades", "taker_buy_volume"])
                .with_columns([pl.col(c).cast(pl.Float64) for c in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume"]])
                .unique(subset="ts").sort("ts"))
        destino.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(destino)
        print(simbolo, mes, df.height)
