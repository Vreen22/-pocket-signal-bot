import os
import time
import threading
import logging
from typing import Dict, List, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pocketoptionapi import PocketOption


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# FASTAPI
# =========================================================

app = FastAPI(title="Pocket Signal Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# SETTINGS
# =========================================================

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

HISTORY_REQUESTS = 3

MAX_CANDLES = 500

TICK_LIMIT = 100


# =========================================================
# STORAGE
# =========================================================

candles_store: Dict[str, List[dict]] = {
    asset: [] for asset in ASSETS
}

status = {
    "connected": False,
    "time_synced": False,
    "message": "Starting...",
    "auto_trade": False,
}

api_instance = None


# =========================================================
# NORMALIZE CANDLE
# =========================================================

def normalize_candle(item: Any):

    try:

        if isinstance(item, dict):

            timestamp = (
                item.get("timestamp")
                or item.get("time")
                or item.get("at")
            )

            if timestamp is None:
                return None

            return {
                "time": int(float(timestamp)),
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "closed": True,
            }

        timestamp = getattr(
            item,
            "timestamp",
            getattr(item, "time", None)
        )

        if timestamp is None:
            return None

        return {
            "time": int(float(timestamp)),
            "open": float(item.open),
            "high": float(item.high),
            "low": float(item.low),
            "close": float(item.close),
            "closed": True,
        }

    except Exception:
        return None


# =========================================================
# NORMALIZE HISTORY
# =========================================================

def normalize_history(data: Any) -> List[dict]:

    result = []

    if data is None:
        return result

    # Pandas DataFrame
    if hasattr(data, "to_dict"):

        try:

            rows = data.to_dict("records")

            for row in rows:

                candle = normalize_candle(row)

                if candle:
                    result.append(candle)

            return result

        except Exception:
            pass

    # Dictionary
    if isinstance(data, dict):

        possible = (
            data.get("candles")
            or data.get("data")
            or data.get("history")
        )

        if isinstance(possible, list):
            data = possible

        else:

            candle = normalize_candle(data)

            if candle:
                result.append(candle)

            return result

    # List / tuple
    if isinstance(data, (list, tuple)):

        for item in data:

            candle = normalize_candle(item)

            if candle:
                result.append(candle)

        return result

    candle = normalize_candle(data)

    if candle:
        result.append(candle)

    return result


# =========================================================
# CLEAN CANDLES
# =========================================================

def clean_candles(items: List[dict]) -> List[dict]:

    unique = {}

    for candle in items:

        try:

            timestamp = int(candle["time"])

            unique[timestamp] = {
                "time": timestamp,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "closed": bool(
                    candle.get("closed", True)
                ),
            }

        except Exception:
            continue

    result = list(unique.values())

    result.sort(
        key=lambda x: x["time"]
    )

    return result[-MAX_CANDLES:]


# =========================================================
# TICK PARSER
# =========================================================

def parse_tick(tick):

    try:

        # Current library returns:
        # (timestamp, price)

        if isinstance(tick, (list, tuple)):

            if len(tick) >= 2:

                timestamp = float(tick[0])
                price = float(tick[1])

                return timestamp, price

        # Dictionary fallback

        if isinstance(tick, dict):

            timestamp = (
                tick.get("timestamp")
                or tick.get("time")
                or tick.get("ts")
            )

            price = (
                tick.get("price")
                or tick.get("close")
                or tick.get("value")
            )

            if timestamp is not None and price is not None:

                return (
                    float(timestamp),
                    float(price)
                )

    except Exception:
        pass

    return None


# =========================================================
# BUILD CANDLES FROM REALTIME TICKS
# =========================================================

def ticks_to_candles(
    api: PocketOption,
    asset: str
) -> List[dict]:

    result = []

    try:

        ticks = api.get_realtime_ticks(
            asset,
            limit=TICK_LIMIT
        )

    except Exception as e:

        logger.warning(
            "Tick read failed %s: %s",
            asset,
            e
        )

        return result

    if not ticks:
        return result

    buckets = {}

    for tick in ticks:

        parsed = parse_tick(tick)

        if not parsed:
            continue

        timestamp, price = parsed

        # 5-second bucket
        bucket = (
            int(timestamp) // PERIOD
        ) * PERIOD

        if bucket not in buckets:

            buckets[bucket] = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }

        else:

            candle = buckets[bucket]

            candle["high"] = max(
                candle["high"],
                price
            )

            candle["low"] = min(
                candle["low"],
                price
            )

            candle["close"] = price

    # Current server candle must remain forming.
    # Only completed buckets are returned.

    try:
        server_time = int(
            api.get_server_timestamp()
        )
    except Exception:
        server_time = int(time.time())

    current_bucket = (
        server_time // PERIOD
    ) * PERIOD

    for bucket, candle in buckets.items():

        if bucket < current_bucket:

            result.append({
                "time": bucket,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "closed": True,
            })

    result.sort(
        key=lambda x: x["time"]
    )

    return result


# =========================================================
# MERGE REALTIME TICKS
# =========================================================

def merge_realtime_ticks(
    api: PocketOption,
    asset: str
):

    try:

        tick_candles = ticks_to_candles(
            api,
            asset
        )

        if not tick_candles:
            return

        existing = candles_store.get(
            asset,
            []
        )

        combined = existing + tick_candles

        cleaned = clean_candles(
            combined
        )

        if len(cleaned) > len(existing):

            candles_store[asset] = cleaned

            logger.info(
                "Realtime merged %s: %s candles",
                asset,
                len(cleaned)
            )

        else:

            # Still update OHLC values if the
            # timestamp already exists.

            candles_store[asset] = cleaned

    except Exception as e:

        logger.warning(
            "Realtime merge failed %s: %s",
            asset,
            e
        )


# =========================================================
# LOAD INITIAL HISTORY
# =========================================================

def load_asset_history(
    api: PocketOption,
    asset: str
):

    logger.info(
        "Loading history: %s",
        asset
    )

    try:

        api.subscribe(
            asset,
            period=PERIOD
        )

        logger.info(
            "Subscribed: %s",
            asset
        )

        time.sleep(1)

        data = api.get_historical_candles(
            asset,
            period=PERIOD,
            offset=HISTORY_OFFSET,
            count_request=HISTORY_REQUESTS,
        )

        candles = normalize_history(
            data
        )

        cleaned = clean_candles(
            candles
        )

        candles_store[asset] = cleaned

        logger.info(
            "Initial %s: %s candles",
            asset,
            len(cleaned)
        )

        # Immediately merge any available ticks
        merge_realtime_ticks(
            api,
            asset
        )

        logger.info(
            "Ready %s: %s candles",
            asset,
            len(candles_store[asset])
        )

        return True

    except Exception as e:

        logger.exception(
            "History error for %s: %s",
            asset,
            e
        )

        return False


# =========================================================
# OPTIONAL HISTORY REFRESH
# =========================================================

def refresh_history(
    api: PocketOption,
    asset: str
):

    try:

        data = api.get_historical_candles(
            asset,
            period=PERIOD,
            offset=HISTORY_OFFSET,
            count_request=1,
        )

        history = normalize_history(
            data
        )

        if history:

            existing = candles_store.get(
                asset,
                []
            )

            combined = existing + history

            candles_store[asset] = clean_candles(
                combined
            )

    except Exception as e:

        logger.warning(
            "History refresh failed %s: %s",
            asset,
            e
        )


# =========================================================
# CONNECT TO POCKET OPTION
# =========================================================

def connect_pocket():

    global api_instance

    if not SSID:

        status["message"] = (
            "PO_SSID is missing"
        )

        logger.error(
            "PO_SSID is missing"
        )

        return

    logger.info(
        "Connecting to Pocket Option..."
    )

    try:

        api = PocketOption(
            SSID
        )

        api_instance = api

        ok, err = api.connect()

        if not ok:

            status["message"] = (
                f"Connection failed: {err}"
            )

            logger.error(
                "Connection failed: %s",
                err
            )

            return

        logger.info(
            "WebSocket connected"
        )

        # -------------------------------------------------
        # Wait for connection + time synchronization
        # -------------------------------------------------

        deadline = time.time() + 30

        while time.time() < deadline:

            connected = api.check_connect()

            synced = api.is_time_synced()

            status["connected"] = bool(
                connected
            )

            status["time_synced"] = bool(
                synced
            )

            if connected and synced:
                break

            time.sleep(0.5)

        if not api.check_connect():

            status["connected"] = False

            status["message"] = (
                "WebSocket not connected"
            )

            logger.error(
                "WebSocket not connected"
            )

            return

        if not api.is_time_synced():

            status["time_synced"] = False

            status["message"] = (
                "Time synchronization failed"
            )

            logger.error(
                "Time synchronization failed"
            )

            return

        status["connected"] = True

        status["time_synced"] = True

        status["message"] = (
            "Connected + time synchronized"
        )

        logger.info(
            "Pocket Option connected successfully"
        )

        logger.info(
            "Time synchronization ready"
        )

        # -------------------------------------------------
        # Initial history
        # -------------------------------------------------

        successful = 0

        for asset in ASSETS:

            if load_asset_history(
                api,
                asset
            ):

                successful += 1

        logger.info(
            "Initial history complete: %s/%s",
            successful,
            len(ASSETS)
        )

        status["message"] = (
            "LIVE DATA READY"
        )

        # -------------------------------------------------
        # LIVE LOOP
        # -------------------------------------------------

        loop_counter = 0

        while True:

            if not api.check_connect():

                status["connected"] = False

                status["message"] = (
                    "Disconnected"
                )

                logger.warning(
                    "Pocket Option disconnected"
                )

                break

            status["connected"] = True

            status["time_synced"] = (
                api.is_time_synced()
            )

            # ---------------------------------------------
            # MAIN: realtime ticks
            # ---------------------------------------------

            for asset in ASSETS:

                merge_realtime_ticks(
                    api,
                    asset
                )

            # ---------------------------------------------
            # Every 30 seconds, refresh history too.
            # It is NOT used to overwrite existing data.
            # ---------------------------------------------

            loop_counter += 1

            if loop_counter >= 6:

                loop_counter = 0

                for asset in ASSETS:

                    refresh_history(
                        api,
                        asset
                    )

                    merge_realtime_ticks(
                        api,
                        asset
                    )

            status["message"] = (
                "LIVE DATA READY"
            )

            time.sleep(5)

    except Exception as e:

        status["connected"] = False

        status["message"] = (
            f"Fatal error: {e}"
        )

        logger.exception(
            "Pocket Option fatal error"
        )


# =========================================================
# START
# =========================================================

@app.on_event("startup")
def startup_event():

    thread = threading.Thread(
        target=connect_pocket,
        daemon=True
    )

    thread.start()


# =========================================================
# ROOT
# =========================================================

@app.get("/")
def root():

    return {
        "service": "Pocket Signal Bot",
        "status": status,
        "assets": ASSETS,
        "auto_trade": False,
    }


# =========================================================
# STATUS
# =========================================================

@app.get("/status")
def get_status():

    return {
        "status": status,
        "auto_trade": False,
    }


# =========================================================
# CANDLES
# =========================================================

@app.get("/candles")
def get_candles(
    asset: str = "EURUSD_otc"
):

    if asset not in candles_store:

        return {
            "asset": asset,
            "candles": [],
            "count": 0,
            "error": "Unsupported asset",
            "auto_trade": False,
        }

    candles = candles_store.get(
        asset,
        []
    )

    return {
        "asset": asset,
        "period": PERIOD,
        "candles": candles,
        "count": len(candles),
        "connected": status["connected"],
        "time_synced": status["time_synced"],
        "auto_trade": False,
    }


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():

    return {
        "ok": True,
        "connected": status["connected"],
        "time_synced": status["time_synced"],
        "auto_trade": False,
    }
