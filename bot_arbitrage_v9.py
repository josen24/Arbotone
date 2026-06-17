"""
Bot Arbitraje Triangular v9 — Binance MAINNET
Base: v6 que funciona + mejoras:
- WebSocket para precios (sin consumir rate limit)
- Rate limiter dinámico (auto-pausa al 80%)
- Timestamp automático (sin errores de tiempo)
"""

import time
import threading
import logging
import asyncio
import json
import websockets
from binance.client import Client
from cerebro_ia import analizar_oportunidad
from guardar_datos import registrar_evento_mercado

# ────────────────────────────────────────────────────────────
#  CREDENCIALES — pon tus keys de mainnet aquí
# ────────────────────────────────────────────────────────────
API_KEY    = "TU_API_KEY_MAINNET"
API_SECRET = "TU_API_SECRET_MAINNET"

# ── Parámetros v9 ───────────────────────────────────────────
TRADE_PCT      = 0.30
TRADE_MAX_USDT = 30.0
TRADE_MIN_USDT = 15.0
MIN_PROFIT_PCT = 0.08
FEE            = 0.001
SCAN_SEC       = 1.0
COOLDOWN_SEC   = 15.0
ORDER_TIMEOUT  = 30
LOG_FILE       = "bot_arbitrage.log"
TIME_SYNC_SEC  = 60
TOP_N_LOG      = 5
WEIGHT_LIMIT   = 6000
WEIGHT_PAUSE   = 0.80

# ── Contrato BSC ─────────────────────────────────────────────
CONTRACT_ENABLED  = True
CONTRACT_ADDRESS  = "TU_CONTRACT_ADDRESS"
BOT_WALLET_PK     = "TU_PRIVATE_KEY"
BSC_RPC           = "https://bsc-dataseed1.binance.org/"

CONTRACT_ABI = [
    {"inputs": [], "name": "depositProfits", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [], "name": "totalInvested", "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
]

# ────────────────────────────────────────────────────────────
#  TRIÁNGULOS — 84 rutas
# ────────────────────────────────────────────────────────────
TRIANGULOS = [
    ("USDT→BNB→SOL→USDT",   [("BNBUSDT","BUY",True),  ("SOLBNB","BUY",True),    ("SOLUSDT","SELL",True)]),
    ("USDT→SOL→BNB→USDT",   [("SOLUSDT","BUY",True),  ("SOLBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→AVAX→USDT",  [("BNBUSDT","BUY",True),  ("AVAXBNB","BUY",True),   ("AVAXUSDT","SELL",True)]),
    ("USDT→AVAX→BNB→USDT",  [("AVAXUSDT","BUY",True), ("AVAXBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→POL→USDT",   [("BNBUSDT","BUY",True),  ("POLBNB","BUY",True),    ("POLUSDT","SELL",True)]),
    ("USDT→POL→BNB→USDT",   [("POLUSDT","BUY",True),  ("POLBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→XRP→USDT",   [("BNBUSDT","BUY",True),  ("XRPBNB","BUY",True),    ("XRPUSDT","SELL",True)]),
    ("USDT→XRP→BNB→USDT",   [("XRPUSDT","BUY",True),  ("XRPBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→DOGE→USDT",  [("BNBUSDT","BUY",True),  ("DOGEBNB","BUY",True),   ("DOGEUSDT","SELL",True)]),
    ("USDT→DOGE→BNB→USDT",  [("DOGEUSDT","BUY",True), ("DOGEBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→ADA→USDT",   [("BNBUSDT","BUY",True),  ("ADABNB","BUY",True),    ("ADAUSDT","SELL",True)]),
    ("USDT→ADA→BNB→USDT",   [("ADAUSDT","BUY",True),  ("ADABNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→DOT→USDT",   [("BNBUSDT","BUY",True),  ("DOTBNB","BUY",True),    ("DOTUSDT","SELL",True)]),
    ("USDT→DOT→BNB→USDT",   [("DOTUSDT","BUY",True),  ("DOTBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→LINK→USDT",  [("BNBUSDT","BUY",True),  ("LINKBNB","BUY",True),   ("LINKUSDT","SELL",True)]),
    ("USDT→LINK→BNB→USDT",  [("LINKUSDT","BUY",True), ("LINKBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→LTC→USDT",   [("BNBUSDT","BUY",True),  ("LTCBNB","BUY",True),    ("LTCUSDT","SELL",True)]),
    ("USDT→LTC→BNB→USDT",   [("LTCUSDT","BUY",True),  ("LTCBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→CAKE→USDT",  [("BNBUSDT","BUY",True),  ("CAKEBNB","BUY",True),   ("CAKEUSDT","SELL",True)]),
    ("USDT→CAKE→BNB→USDT",  [("CAKEUSDT","BUY",True), ("CAKEBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→NEAR→USDT",  [("BNBUSDT","BUY",True),  ("NEARBNB","BUY",True),   ("NEARUSDT","SELL",True)]),
    ("USDT→NEAR→BNB→USDT",  [("NEARUSDT","BUY",True), ("NEARBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→INJ→USDT",   [("BNBUSDT","BUY",True),  ("INJBNB","BUY",True),    ("INJUSDT","SELL",True)]),
    ("USDT→INJ→BNB→USDT",   [("INJUSDT","BUY",True),  ("INJBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→UNI→USDT",   [("BNBUSDT","BUY",True),  ("UNIBNB","BUY",True),    ("UNIUSDT","SELL",True)]),
    ("USDT→UNI→BNB→USDT",   [("UNIUSDT","BUY",True),  ("UNIBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→ATOM→USDT",  [("BNBUSDT","BUY",True),  ("ATOMBNB","BUY",True),   ("ATOMUSDT","SELL",True)]),
    ("USDT→ATOM→BNB→USDT",  [("ATOMUSDT","BUY",True), ("ATOMBNB","SELL",True),  ("BNBUSDT","SELL",True)]),
    ("USDT→BNB→TRX→USDT",   [("BNBUSDT","BUY",True),  ("TRXBNB","BUY",True),    ("TRXUSDT","SELL",True)]),
    ("USDT→TRX→BNB→USDT",   [("TRXUSDT","BUY",True),  ("TRXBNB","SELL",True),   ("BNBUSDT","SELL",True)]),
    ("USDT→ETH→SOL→USDT",   [("ETHUSDT","BUY",True),  ("SOLETH","BUY",True),    ("SOLUSDT","SELL",True)]),
    ("USDT→SOL→ETH→USDT",   [("SOLUSDT","BUY",True),  ("SOLETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→POL→USDT",   [("ETHUSDT","BUY",True),  ("POLETH","BUY",True),    ("POLUSDT","SELL",True)]),
    ("USDT→POL→ETH→USDT",   [("POLUSDT","BUY",True),  ("POLETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→XRP→USDT",   [("ETHUSDT","BUY",True),  ("XRPETH","BUY",True),    ("XRPUSDT","SELL",True)]),
    ("USDT→XRP→ETH→USDT",   [("XRPUSDT","BUY",True),  ("XRPETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→ADA→USDT",   [("ETHUSDT","BUY",True),  ("ADAETH","BUY",True),    ("ADAUSDT","SELL",True)]),
    ("USDT→ADA→ETH→USDT",   [("ADAUSDT","BUY",True),  ("ADAETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→LINK→USDT",  [("ETHUSDT","BUY",True),  ("LINKETH","BUY",True),   ("LINKUSDT","SELL",True)]),
    ("USDT→LINK→ETH→USDT",  [("LINKUSDT","BUY",True), ("LINKETH","SELL",True),  ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→LTC→USDT",   [("ETHUSDT","BUY",True),  ("LTCETH","BUY",True),    ("LTCUSDT","SELL",True)]),
    ("USDT→LTC→ETH→USDT",   [("LTCUSDT","BUY",True),  ("LTCETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→DOGE→USDT",  [("ETHUSDT","BUY",True),  ("DOGEETH","BUY",True),   ("DOGEUSDT","SELL",True)]),
    ("USDT→DOGE→ETH→USDT",  [("DOGEUSDT","BUY",True), ("DOGEETH","SELL",True),  ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→AVAX→USDT",  [("ETHUSDT","BUY",True),  ("AVAXETH","BUY",True),   ("AVAXUSDT","SELL",True)]),
    ("USDT→AVAX→ETH→USDT",  [("AVAXUSDT","BUY",True), ("AVAXETH","SELL",True),  ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→UNI→USDT",   [("ETHUSDT","BUY",True),  ("UNIETH","BUY",True),    ("UNIUSDT","SELL",True)]),
    ("USDT→UNI→ETH→USDT",   [("UNIUSDT","BUY",True),  ("UNIETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→NEAR→USDT",  [("ETHUSDT","BUY",True),  ("NEARETH","BUY",True),   ("NEARUSDT","SELL",True)]),
    ("USDT→NEAR→ETH→USDT",  [("NEARUSDT","BUY",True), ("NEARETH","SELL",True),  ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→ATOM→USDT",  [("ETHUSDT","BUY",True),  ("ATOMETH","BUY",True),   ("ATOMUSDT","SELL",True)]),
    ("USDT→ATOM→ETH→USDT",  [("ATOMUSDT","BUY",True), ("ATOMETH","SELL",True),  ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→DOT→USDT",   [("ETHUSDT","BUY",True),  ("DOTETH","BUY",True),    ("DOTUSDT","SELL",True)]),
    ("USDT→DOT→ETH→USDT",   [("DOTUSDT","BUY",True),  ("DOTETH","SELL",True),   ("ETHUSDT","SELL",True)]),
    ("USDT→BTC→ETH→USDT",   [("BTCUSDT","BUY",True),  ("ETHBTC","BUY",True),    ("ETHUSDT","SELL",True)]),
    ("USDT→ETH→BTC→USDT",   [("ETHUSDT","BUY",True),  ("ETHBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→XRP→USDT",   [("BTCUSDT","BUY",True),  ("XRPBTC","BUY",True),    ("XRPUSDT","SELL",True)]),
    ("USDT→XRP→BTC→USDT",   [("XRPUSDT","BUY",True),  ("XRPBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→ADA→USDT",   [("BTCUSDT","BUY",True),  ("ADABTC","BUY",True),    ("ADAUSDT","SELL",True)]),
    ("USDT→ADA→BTC→USDT",   [("ADAUSDT","BUY",True),  ("ADABTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→DOT→USDT",   [("BTCUSDT","BUY",True),  ("DOTBTC","BUY",True),    ("DOTUSDT","SELL",True)]),
    ("USDT→DOT→BTC→USDT",   [("DOTUSDT","BUY",True),  ("DOTBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→LINK→USDT",  [("BTCUSDT","BUY",True),  ("LINKBTC","BUY",True),   ("LINKUSDT","SELL",True)]),
    ("USDT→LINK→BTC→USDT",  [("LINKUSDT","BUY",True), ("LINKBTC","SELL",True),  ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→LTC→USDT",   [("BTCUSDT","BUY",True),  ("LTCBTC","BUY",True),    ("LTCUSDT","SELL",True)]),
    ("USDT→LTC→BTC→USDT",   [("LTCUSDT","BUY",True),  ("LTCBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→AVAX→USDT",  [("BTCUSDT","BUY",True),  ("AVAXBTC","BUY",True),   ("AVAXUSDT","SELL",True)]),
    ("USDT→AVAX→BTC→USDT",  [("AVAXUSDT","BUY",True), ("AVAXBTC","SELL",True),  ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→DOGE→USDT",  [("BTCUSDT","BUY",True),  ("DOGEBTC","BUY",True),   ("DOGEUSDT","SELL",True)]),
    ("USDT→DOGE→BTC→USDT",  [("DOGEUSDT","BUY",True), ("DOGEBTC","SELL",True),  ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→SOL→USDT",   [("BTCUSDT","BUY",True),  ("SOLBTC","BUY",True),    ("SOLUSDT","SELL",True)]),
    ("USDT→SOL→BTC→USDT",   [("SOLUSDT","BUY",True),  ("SOLBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→NEAR→USDT",  [("BTCUSDT","BUY",True),  ("NEARBTC","BUY",True),   ("NEARUSDT","SELL",True)]),
    ("USDT→NEAR→BTC→USDT",  [("NEARUSDT","BUY",True), ("NEARBTC","SELL",True),  ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→UNI→USDT",   [("BTCUSDT","BUY",True),  ("UNIBTC","BUY",True),    ("UNIUSDT","SELL",True)]),
    ("USDT→UNI→BTC→USDT",   [("UNIUSDT","BUY",True),  ("UNIBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→ATOM→USDT",  [("BTCUSDT","BUY",True),  ("ATOMBTC","BUY",True),   ("ATOMUSDT","SELL",True)]),
    ("USDT→ATOM→BTC→USDT",  [("ATOMUSDT","BUY",True), ("ATOMBTC","SELL",True),  ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→INJ→USDT",   [("BTCUSDT","BUY",True),  ("INJBTC","BUY",True),    ("INJUSDT","SELL",True)]),
    ("USDT→INJ→BTC→USDT",   [("INJUSDT","BUY",True),  ("INJBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
    ("USDT→BTC→TRX→USDT",   [("BTCUSDT","BUY",True),  ("TRXBTC","BUY",True),    ("TRXUSDT","SELL",True)]),
    ("USDT→TRX→BTC→USDT",   [("TRXUSDT","BUY",True),  ("TRXBTC","SELL",True),   ("BTCUSDT","SELL",True)]),
]

# ────────────────────────────────────────────────────────────
#  LOGGING
# ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ARB-v9")

# ────────────────────────────────────────────────────────────
#  ESTADO GLOBAL
# ────────────────────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.total_pnl        = 0.0
        self.trades           = 0
        self.wins             = 0
        self.losses           = 0
        self.balance_usdt     = 0.0
        self.running          = True
        self.en_operacion     = False
        self.last_trade       = 0
        self.lock             = threading.Lock()
        self.prices           = {}
        self.lot_sizes        = {}
        self.time_offset      = 0
        self.profit_pendiente = 0.0
        self.used_weight      = 0
        self.weight_reset     = time.time()

state  = BotState()
client = Client(API_KEY, API_SECRET, testnet=False)

# ────────────────────────────────────────────────────────────
#  RATE LIMITER INTELIGENTE
# ────────────────────────────────────────────────────────────
def check_rate_limit(weight: int = 2):
    ahora = time.time()
    with state.lock:
        if ahora - state.weight_reset > 60:
            state.used_weight  = 0
            state.weight_reset = ahora
        state.used_weight += weight
        pct = state.used_weight / WEIGHT_LIMIT

    if pct >= WEIGHT_PAUSE:
        pausa = 10 + (pct - WEIGHT_PAUSE) * 100
        log.warning(f"⚠️  Rate limit {pct*100:.0f}% ({state.used_weight}/{WEIGHT_LIMIT}) — pausando {pausa:.1f}s")
        time.sleep(pausa)

def actualizar_weight(headers):
    w = headers.get("x-mbx-used-weight-1m") or headers.get("X-MBX-USED-WEIGHT-1M")
    if w:
        with state.lock:
            state.used_weight = int(w)

# ────────────────────────────────────────────────────────────
#  SINCRONIZACIÓN DE TIEMPO
# ────────────────────────────────────────────────────────────
def sincronizar_tiempo():
    try:
        server_time = client.get_server_time()["serverTime"]
        local_time  = int(time.time() * 1000)
        offset      = server_time - local_time
        with state.lock:
            state.time_offset = offset
        client.timestamp_offset = offset
        log.info(f"⏱️  Tiempo sincronizado | Offset: {offset}ms")
    except Exception as e:
        log.error(f"Error sincronizando tiempo: {e}")

def time_sync_updater():
    while state.running:
        sincronizar_tiempo()
        time.sleep(TIME_SYNC_SEC)

def get_timestamp():
    with state.lock:
        offset = state.time_offset
    return int(time.time() * 1000) + offset

# ────────────────────────────────────────────────────────────
#  WEBSOCKET DE PRECIOS — sin consumir rate limit
# ────────────────────────────────────────────────────────────
def ws_price_thread():
    asyncio.run(ws_price_stream())

async def ws_price_stream():
    simbolos = set()
    for _, pasos in TRIANGULOS:
        for s, _, _ in pasos:
            simbolos.add(s.lower() + "@bookTicker")
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(simbolos)
    log.info(f"📡 WebSocket precios conectando ({len(simbolos)} pares)...")
    while state.running:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("✅ WebSocket precios conectado")
                async for msg in ws:
                    data = json.loads(msg)
                    d    = data.get("data", data)
                    sym  = d.get("s")
                    if sym:
                        with state.lock:
                            state.prices[sym] = {
                                "bid": float(d["b"]),
                                "ask": float(d["a"])
                            }
        except Exception as e:
            log.error(f"WebSocket precios error: {e} — reconectando en 3s")
            await asyncio.sleep(3)

# ────────────────────────────────────────────────────────────
#  BALANCE
# ────────────────────────────────────────────────────────────
def obtener_balance_usdt() -> float:
    try:
        check_rate_limit(weight=10)
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception as e:
        log.error(f"Error obteniendo balance: {e}")
    return 0.0

def balance_updater():
    while state.running:
        bal = obtener_balance_usdt()
        with state.lock:
            state.balance_usdt = bal
        time.sleep(30)

# ────────────────────────────────────────────────────────────
#  LOT SIZES
# ────────────────────────────────────────────────────────────
def cargar_lot_sizes():
    try:
        info = client.get_exchange_info()
        for s in info["symbols"]:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    state.lot_sizes[s["symbol"]] = float(f["stepSize"])
        log.info(f"✅ LOT_SIZE cargados: {len(state.lot_sizes)} pares")
    except Exception as e:
        log.error(f"Error cargando lot sizes: {e}")

def ajustar_qty(symbol, qty):
    step = state.lot_sizes.get(symbol, 0.00001)
    if step == 0:
        step = 0.00001
    decimals = len(f"{step:.10f}".rstrip("0").split(".")[-1])
    return round(int(qty / step) * step, decimals)

# ────────────────────────────────────────────────────────────
#  CONTRATO BSC
# ────────────────────────────────────────────────────────────
contract = None

def init_contract():
    global contract
    if not CONTRACT_ENABLED:
        return
    try:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(BSC_RPC))
        if not w3.is_connected():
            log.error("❌ No se pudo conectar a BSC")
            return
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=CONTRACT_ABI
        )
        log.info(f"✅ Contrato conectado: {CONTRACT_ADDRESS[:12]}...")
    except Exception as e:
        log.error(f"Error iniciando contrato: {e}")

def depositar_ganancia_contrato(ganancia_usdt: float):
    if not CONTRACT_ENABLED or contract is None:
        return
    with state.lock:
        state.profit_pendiente += ganancia_usdt
        pendiente = state.profit_pendiente
    if pendiente < 1.0:
        return
    try:
        from web3 import Web3
        w3       = Web3(Web3.HTTPProvider(BSC_RPC))
        account  = w3.eth.account.from_key(BOT_WALLET_PK)
        total    = contract.functions.totalInvested().call()
        if total == 0:
            return
        with state.lock:
            p = state.prices.copy()
        bnb_price = p.get("BNBUSDT", {}).get("bid", 0)
        if bnb_price == 0:
            return
        ganancia_bnb = pendiente / bnb_price
        ganancia_wei = w3.to_wei(ganancia_bnb, "ether")
        nonce = w3.eth.get_transaction_count(account.address)
        tx = contract.functions.depositProfits().build_transaction({
            "from": account.address, "value": ganancia_wei,
            "gas": 200000, "gasPrice": w3.to_wei("5", "gwei"),
            "nonce": nonce, "chainId": 56
        })
        signed  = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] == 1:
            log.info(f"✅ Ganancia enviada: ${pendiente:.4f} USDT | TX: {tx_hash.hex()[:16]}...")
            with state.lock:
                state.profit_pendiente = 0.0
    except Exception as e:
        log.error(f"Error enviando al contrato: {e}")

# ────────────────────────────────────────────────────────────
#  CÁLCULO DE RENTABILIDAD
# ────────────────────────────────────────────────────────────
def calcular_triangulo(nombre, pasos, trade_usdt: float) -> float | None:
    try:
        with state.lock:
            p = state.prices.copy()
        symbols = [paso[0] for paso in pasos]
        if not all(s in p for s in symbols):
            return None
        f     = 1 - FEE
        monto = trade_usdt
        for symbol, side, _ in pasos:
            bid = p[symbol]["bid"]
            ask = p[symbol]["ask"]
            if bid == 0 or ask == 0:
                return None
            if side == "BUY":
                monto = (monto / ask) * f
            else:
                monto = monto * bid * f
        return ((monto - trade_usdt) / trade_usdt) * 100
    except Exception:
        return None

# ────────────────────────────────────────────────────────────
#  EJECUCIÓN DE ORDEN
# ────────────────────────────────────────────────────────────
def ejecutar_orden(symbol, side, qty) -> dict | None:
    try:
        check_rate_limit(weight=1)
        qty_adj = ajustar_qty(symbol, qty)
        if qty_adj <= 0:
            return None
        order = client.create_order(
            symbol    = symbol,
            side      = side,
            type      = "MARKET",
            quantity  = qty_adj,
            timestamp = get_timestamp()
        )
        fills  = order.get("fills", [])
        precio = float(fills[0]["price"]) if fills else 0
        log.info(f"    ✅ {side} {qty_adj} {symbol} @ ${precio:,.4f}")
        return order
    except Exception as e:
        log.error(f"    ❌ {side} {symbol}: {e}")
        return None

# ────────────────────────────────────────────────────────────
#  EJECUCIÓN DE TRIÁNGULO
# ────────────────────────────────────────────────────────────
def ejecutar_triangulo(nombre, pasos, trade_usdt: float) -> tuple[bool, float]:
    try:
        with state.lock:
            p = state.prices.copy()
        balance_antes = obtener_balance_usdt()
        log.info(f"  💰 Balance antes: ${balance_antes:,.4f}")
        f     = 1 - FEE
        monto = trade_usdt
        for symbol, side, _ in pasos:
            bid = p[symbol]["bid"]
            ask = p[symbol]["ask"]
            qty = (monto / ask) * f if side == "BUY" else monto
            res = ejecutar_orden(symbol, side, qty)
            if not res:
                log.error(f"  ❌ Falló {side} {symbol} — abortando")
                return False, 0.0
            if side == "BUY":
                monto = float(res.get("executedQty", qty))
            else:
                monto = float(res.get("cummulativeQuoteQty", qty * bid * f))
        time.sleep(2)
        balance_despues = obtener_balance_usdt()
        ganancia_real   = balance_despues - balance_antes
        log.info(f"  📐 P&L real: ${ganancia_real:+.4f}")
        return True, ganancia_real
    except Exception as e:
        log.error(f"Error ejecutando {nombre}: {e}")
        return False, 0.0

# ────────────────────────────────────────────────────────────
#  MOTOR PRINCIPAL
# ────────────────────────────────────────────────────────────
def arbitrage_engine():
    log.info("🔍 Motor de arbitraje v9 iniciado")
    for _ in range(20):
        with state.lock:
            n = len(state.prices)
        if n > 10:
            break
        time.sleep(1)

    with state.lock:
        n = len(state.prices)
    log.info(f"✅ Precios listos — {n} pares | Escaneando {len(TRIANGULOS)} triángulos")

    while state.running:
        try:
            with state.lock:
                if state.en_operacion:
                    time.sleep(SCAN_SEC)
                    continue

            trade_usdt = max(min(state.balance_usdt * TRADE_PCT, TRADE_MAX_USDT), TRADE_MIN_USDT)
            min_profit = trade_usdt * (MIN_PROFIT_PCT / 100)

            resultados = []
            for nombre, pasos in TRIANGULOS:
                profit = calcular_triangulo(nombre, pasos, trade_usdt)
                if profit is not None:
                    resultados.append((profit, nombre, pasos))

            if not resultados:
                time.sleep(SCAN_SEC)
                continue

            resultados.sort(key=lambda x: x[0], reverse=True)
            top = resultados[:TOP_N_LOG]
            top_str = " | ".join(
                f"{r[1].split('→')[1]}/{r[1].split('→')[2]}: {r[0]:+.4f}%"
                for r in top
            )

            with state.lock:
                peso    = state.used_weight
                pnl     = state.total_pnl
                bal     = state.balance_usdt
                w       = state.wins
                l       = state.losses
                contrato = state.profit_pendiente

            log.info(
                f"📊 TOP5: {top_str} | Capital: ${trade_usdt:,.0f} | "
                f"USDT: ${bal:,.2f} | P&L: ${pnl:+.2f} | W/L: {w}/{l} | "
                f"Weight: {peso}/{WEIGHT_LIMIT} | Contrato: ${contrato:.4f}"
            )

            mejor_profit, mejor_nombre, mejor_pasos = resultados[0]
            ganancia_est = (mejor_profit / 100) * trade_usdt
            cooldown_ok  = (time.time() - state.last_trade) > COOLDOWN_SEC

            with state.lock:
                p2 = state.prices.copy()
            sym0 = mejor_pasos[0][0]
            bid0 = p2.get(sym0, {}).get("bid", 0)
            ask0 = p2.get(sym0, {}).get("ask", 0)
            vol0 = trade_usdt / ask0 if ask0 > 0 else 0
            aprobado_ia = analizar_oportunidad(bid0, ask0, vol0)

            if ganancia_est > min_profit and cooldown_ok and aprobado_ia:
                log.info(f"🚀 EJECUTANDO: {mejor_nombre} | Est: ${ganancia_est:+.4f} ({mejor_profit:+.4f}%)")
                with state.lock:
                    state.en_operacion = True

                exito, ganancia_real = ejecutar_triangulo(mejor_nombre, mejor_pasos, trade_usdt)

                with state.lock:
                    state.trades      += 1
                    state.last_trade   = time.time()
                    state.en_operacion = False
                    if exito:
                        state.total_pnl += ganancia_real
                        if ganancia_real >= 0:
                            state.wins += 1
                        else:
                            state.losses += 1

                if exito and ganancia_real > 0:
                    depositar_ganancia_contrato(ganancia_real)

        except Exception as e:
            log.error(f"Error en motor: {e}")
            with state.lock:
                state.en_operacion = False

        time.sleep(SCAN_SEC)

# ────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────
def main():
    log.info("═" * 65)
    log.info("  BOT ARBITRAJE v9 — WebSocket + Rate Limiter Inteligente")
    log.info(f"  Triángulos : {len(TRIANGULOS)} rutas")
    log.info(f"  Scan       : {SCAN_SEC}s | Cooldown: {COOLDOWN_SEC}s")
    log.info(f"  Min profit : {MIN_PROFIT_PCT}% | Fee: {FEE*100}%/orden")
    log.info(f"  Rate limit : auto-pausa al {WEIGHT_PAUSE*100:.0f}% de {WEIGHT_LIMIT}/min")
    log.info(f"  Contrato   : {'ACTIVO' if CONTRACT_ENABLED else 'INACTIVO'}")
    log.info("═" * 65)

    sincronizar_tiempo()
    time.sleep(1)
    init_contract()
    cargar_lot_sizes()

    bal = obtener_balance_usdt()
    with state.lock:
        state.balance_usdt = bal
    log.info(f"💰 Balance inicial: ${bal:,.2f} USDT")

    threading.Thread(target=ws_price_thread,   daemon=True).start()
    threading.Thread(target=balance_updater,   daemon=True).start()
    threading.Thread(target=time_sync_updater, daemon=True).start()
    threading.Thread(target=arbitrage_engine,  daemon=True).start()

    log.info("✅ Bot v9 iniciado. Ctrl+C para detener.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo bot...")
        state.running = False
        log.info("═" * 65)
        log.info("  RESUMEN FINAL")
        log.info(f"  Trades  : {state.trades}")
        log.info(f"  Wins    : {state.wins}  |  Losses: {state.losses}")
        log.info(f"  P&L acumulado: ${state.total_pnl:+.4f} USDT")
        log.info("═" * 65)

if __name__ == "__main__":
    main()
