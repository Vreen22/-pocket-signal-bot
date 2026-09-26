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

app = FastAPI(
    title="Pocket Signal Bot",
    version="1.0"
)

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

# 5-second candles
PERIOD = 5

# Large offset helps avoid Pocket Option history timeout
HISTORY_OFFSET = 45000

# Multiple history pages
HISTORY_REQUESTS = 3

# Maximum candles sent to frontend
MAX_CANDLES = 500


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
# CANDLE NORMALIZER
# =========================================================

def normalize_candle(item: Any):
    """
    Convert PocketOption candle data into our standard format.
    """

    try:

        # ---------------------------------------------
        # Dictionary
        # ---------------------------------------------

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


        # ---------------------------------------------
        # Object with attributes
        # ---------------------------------------------

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

    except Exception as e:

        logger.debug(
            "Candle normalization failed: %s",
            e
        )

        return None


# =========================================================
# NORMALIZE HISTORY RESULT
# =========================================================

def normalize_history(data: Any) -> List[dict]:

    result = []

    if data is None:
        return result


    # ---------------------------------------------
    # Pandas DataFrame
    # ---------------------------------------------

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


    # ---------------------------------------------
    # Dictionary containing candles
    # ---------------------------------------------

    if isinstance(data, dict):

        # Sometimes API may return nested data
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


    # ---------------------------------------------
    # List / tuple
    # ---------------------------------------------

    if isinstance(data, (list, tuple)):

        for item in data:

            candle = normalize_candle(item)

            if candle:
                result.append(candle)

        return result


    # ---------------------------------------------
    # Single object
    # ---------------------------------------------

    candle = normalize_candle(data)

    if candle:
        result.append(candle)

    return result


# =========================================================
# REMOVE DUPLICATES
# =========================================================

def clean_candles(items: List[dict]) -> List[dict]:

    unique = {}

    for candle in items:

        try:
            timestamp = int(candle["time"])

            unique[timestamp] = candle

        except Exception:
            continue


    result = list(unique.values())

    result.sort(
        key=lambda x: x["time"]
    )

    return result[-MAX_CANDLES:]


# =========================================================
# LOAD HISTORY
# =========================================================

def load_asset_history(
    api: PocketOption,
    asset: str
):

    logger.info(
        "Loading history: %s",
        asset
    )

    all_candles = []

    try:

        # Subscribe first
        api.subscribe(
            asset,
            period=PERIOD
        )

        logger.info(
            "Subscribed: %s",
            asset
        )

        # Give stream a moment
        time.sleep(1)


        # -----------------------------------------
        # Multiple history requests
        # -----------------------------------------

        data = api.get_historical_candles(
            asset,
            period=PERIOD,
            offset=HISTORY_OFFSET,
            count_request=HISTORY_REQUESTS,
        )


        candles = normalize_history(data)

        if candles:

            all_candles.extend(candles)


        cleaned = clean_candles(
            all_candles
        )


        candles_store[asset] = cleaned


        logger.info(
            "Loaded %s closed candles for %s",
            len(cleaned),
            asset
        )


        if len(cleaned) >= 200:

            logger.info(
                "READY: %s has enough candles for signal engine",
                asset
            )

        else:

            logger.warning(
                "Only %s candles available for %s; need 200",
                len(cleaned),
                asset
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
# UPDATE ONE ASSET
# =========================================================

def update_asset(
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


        candles = normalize_history(data)


        if candles:

            candles = clean_candles(
                candles
            )

            if candles:

                candles_store[asset] = candles


                logger.info(
                    "Updated %s: %s candles",
                    asset,
                    len(candles)
                )


    except Exception as e:

        logger.warning(
            "Update failed %s: %s",
            asset,
            e
        )


# =========================================================
# POCKET OPTION CONNECTION
# =========================================================

def connect_pocket():

    global api_instance


    if not SSID:

        status["message"] = "PO_SSID is missing"

        logger.error(
            "PO_SSID is missing"
        )

        return


    logger.info(
        "Connecting to Pocket Option..."
    )


    try:

        api = PocketOption(SSID)

        api_instance = api


        # -----------------------------------------
        # Connect
        # -----------------------------------------

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


        # -----------------------------------------
        # Wait for connection + time sync
        # -----------------------------------------

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


        # -----------------------------------------
        # Verify connection
        # -----------------------------------------

        if not api.check_connect():

            status["connected"] = False

            status["message"] = (
                "WebSocket not connected"
            )

            logger.error(
                "WebSocket is not connected"
            )

            return


        # -----------------------------------------
        # Verify time synchronization
        # -----------------------------------------

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


        # =================================================
        # INITIAL HISTORY
        # =================================================

        successful_assets = 0


        for asset in ASSETS:

            if load_asset_history(
                api,
                asset
            ):

                successful_assets += 1


        logger.info(
            "Initial history complete: %s/%s assets",
            successful_assets,
            len(ASSETS)
        )


        status["message"] = (
            "LIVE DATA READY"
        )


        # =================================================
        # LIVE UPDATE LOOP
        # =================================================

        while True:


            # -----------------------------------------
            # Check connection
            # -----------------------------------------

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


            # -----------------------------------------
            # Update all assets
            # -----------------------------------------

            for asset in ASSETS:

                update_asset(
                    api,
                    asset
                )


            status["message"] = (
                "LIVE DATA READY"
            )


            # Update every 5 seconds
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
# START BACKGROUND THREAD
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
