import os
import time
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pocketoptionapi import PocketOption

app = FastAPI(title="Pocket Signal Live Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

SSID = os.getenv("PO_SSID")

api = None
connected = False
last_error = None

cache = {}
lock = threading.Lock()


def connect_pocket():
    global api, connected, last_error

    if not SSID:
        last_error = "PO_SSID environment variable is missing"
        print(last_error)
        return

    try:
        print("Connecting to Pocket Option...")

        api = PocketOption(SSID)

        ok, err = api.connect()

        if not ok:
            connected = False
            last_error = str(err)
            print("Connection failed:", err)
            return

        while not (
            api.check_connect()
            and api.is_time_synced()
        ):
            time.sleep(0.5)

        connected = True
        last_error = None

        print("Pocket Option connected")

    except Exception as e:
        connected = False
        last_error = str(e)
        print("Connector error:", e)


def get_candles_from_pocket(
    asset="EURUSD_otc",
    period=5
):
    global connected, last_error

    if not api or not connected:
        return []

    try:
        api.subscribe(
            asset,
            period=period
        )

        df = api.get_historical_candles(
            asset,
            period=period,
            offset=45000,
            count_request=1
        )

        if df is None or len(df) == 0:
            return []

        result = []

        now = int(
            datetime.now(
                timezone.utc
            ).timestamp()
        )

        for _, row in df.iterrows():

            ts = int(row["timestamp"])

            # Only CLOSED candles
            if ts + period > now:
                continue

            result.append({
                "time": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "closed": True
            })

        return result[-500:]

    except Exception as e:
        last_error = str(e)
        print("Candle error:", e)
        return []


def connector_loop():

    global cache

    while True:

        try:

            if not connected:

                connect_pocket()

            if connected:

                assets = [
                    "EURUSD_otc",
                    "GBPUSD_otc",
                    "USDJPY_otc",
                    "XAUUSD_otc",
                    "BTCUSD_otc"
                ]

                for asset in assets:

                    candles = get_candles_from_pocket(
                        asset,
                        5
                    )

                    if candles:

                        with lock:
                            cache[asset] = candles

                        print(
                            asset,
                            "closed candles:",
                            len(candles)
                        )

        except Exception as e:

            print(
                "Loop error:",
                e
            )

        time.sleep(3)


@app.on_event("startup")
def startup():

    thread = threading.Thread(
        target=connector_loop,
        daemon=True
    )

    thread.start()


@app.get("/")
def root():

    return {
        "status": "LIVE SIGNAL BACKEND",
        "connected": connected,
        "auto_trade": False,
        "candles": {
            k: len(v)
            for k, v in cache.items()
        }
    }


@app.get("/status")
def status():

    return {
        "connected": connected,
        "auto_trade": False,
        "last_error": last_error,
        "assets": {
            k: len(v)
            for k, v in cache.items()
        }
    }


@app.get("/candles")
def candles(
    asset: str = Query(
        "EURUSD_otc"
    )
):

    with lock:
        data = cache.get(
            asset,
            []
        )

    return {
        "connected": connected,
        "asset": asset,
        "candles": data,
        "auto_trade": False
    }
