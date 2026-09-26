import os
import time
import threading
import logging
from typing import Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pocketoptionapi import PocketOption


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

app = FastAPI(title="Pocket Signal Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

SSID = os.getenv("PO_SSID", "").strip()

ASSETS = [
    "EURUSD_otc",
    "GBPUSD_otc",
    "USDJPY_otc",
    "XAUUSD_otc",
    "BTCUSD_otc",
]

PERIOD = 5
HISTORY_OFFSET = 45000
MAX_CANDLES = 500

candles_store: Dict[str, List[dict]] = {
    asset: [] for asset in ASSETS
}

status = {
    "connected": False,
    "time_synced": False,
    "message": "Starting...",
    "auto_trade": False,
}


def normalize_candle(c):
    try:
        if isinstance(c, dict):
            return {
                "time": int(c.get("timestamp", c.get("time", 0))),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "closed": True,
            }

        return {
            "time": int(c.timestamp),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "closed": True,
        }

    except Exception:
        return None


def connect_pocket():
    if not SSID:
        status["message"] = "PO_SSID is missing"
        logging.error("PO_SSID is missing")
        return

    logging.info("Connecting to Pocket Option...")

    try:
        api = PocketOption(SSID)

        ok, err = api.connect()

        if not ok:
            status["message"] = f"Connection failed: {err}"
            logging.error("Pocket Option connection failed: %s", err)
            return

        logging.info("WebSocket connected")

        # Wait for connection + time synchronization
        deadline = time.time() + 30

        while time.time() < deadline:
            connected = api.check_connect()
            synced = api.is_time_synced()

            status["connected"] = bool(connected)
            status["time_synced"] = bool(synced)

            if connected and synced:
                break

            time.sleep(0.5)

        if not api.check_connect():
            status["message"] = "WebSocket not connected"
            logging.error("WebSocket is not connected")
            return

        if not api.is_time_synced():
            status["message"] = "Time synchronization failed"
            logging.error("Time synchronization failed")
            return

        status["message"] = "Connected + time synchronized"

        logging.info("Pocket Option connected successfully")
        logging.info("Time synchronization ready")

        # Subscribe to assets first
        for asset in ASSETS:
            try:
                api.subscribe(asset, period=PERIOD)
                logging.info("Subscribed: %s", asset)
            except Exception as e:
                logging.error("Subscribe failed %s: %s", asset, e)

        time.sleep(2)

        # Historical candles
        for asset in ASSETS:
            try:
                logging.info("Loading history: %s", asset)

                data = api.get_historical_candles(
                    asset,
                    period=PERIOD,
                    offset=HISTORY_OFFSET,
                    count_request=1,
                )

                if not data:
                    logging.warning("No historical data: %s", asset)
                    continue

                result = []

                for item in data:
                    candle = normalize_candle(item)

                    if candle:
                        result.append(candle)

                result.sort(key=lambda x: x["time"])

                candles_store[asset] = result[-MAX_CANDLES:]

                logging.info(
                    "Loaded %s candles for %s",
                    len(candles_store[asset]),
                    asset
                )

            except Exception as e:
                logging.exception(
                    "History error for %s: %s",
                    asset,
                    e
                )

        status["message"] = "LIVE DATA READY"

        # Keep updating latest candles
        while True:
            if not api.check_connect():
                status["connected"] = False
                status["message"] = "Disconnected"
                logging.warning("Pocket Option disconnected")
                break

            status["connected"] = True
            status["time_synced"] = api.is_time_synced()

            for asset in ASSETS:
                try:
                    data = api.get_historical_candles(
                        asset,
                        period=PERIOD,
                        offset=45000,
                        count_request=1,
                    )

                    if data:
                        result = []

                        for item in data:
                            candle = normalize_candle(item)

                            if candle:
                                result.append(candle)

                        result.sort(key=lambda x: x["time"])

                        if result:
                            candles_store[asset] = result[-MAX_CANDLES:]

                except Exception as e:
                    logging.warning(
                        "Update failed %s: %s",
                        asset,
                        e
                    )

            time.sleep(5)

    except Exception as e:
        status["connected"] = False
        status["message"] = f"Fatal error: {e}"
        logging.exception("Pocket Option fatal error")


@app.on_event("startup")
def startup_event():
    thread = threading.Thread(
        target=connect_pocket,
        daemon=True
    )
    thread.start()


@app.get("/")
def root():
    return {
        "service": "Pocket Signal Bot",
        "status": status,
        "auto_trade": False,
    }


@app.get("/status")
def get_status():
    return {
        "status": status,
        "auto_trade": False,
    }


@app.get("/candles")
def get_candles(asset: str = "EURUSD_otc"):
    if asset not in candles_store:
        return {
            "asset": asset,
            "candles": [],
            "error": "Unsupported asset"
        }

    return {
        "asset": asset,
        "period": PERIOD,
        "candles": candles_store[asset],
        "count": len(candles_store[asset]),
        "auto_trade": False,
    }
