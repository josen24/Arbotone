"""
Bot Arbitraje v10 — Cash & Carry: Spot vs Futuros Perpetuos
Estrategia: cuando Futuros > Spot por umbral mínimo →
            BUY Spot + SHORT Futuros simultáneo
            Cerrar ambos cuando spread converge
Pares: BTC, ETH, BNB, SOL, DOGE
Capital inicial: ~$100
"""

import time
import threading
import logging
import asyncio
import json
import os
import websockets
from binance.client import Client
from binance.enums import *
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────
#  CREDENCIALES
#  Cambiar a BINANCE_API_KEY / BINANCE_API_SECRET para mainnet
# ────────────────────────────────────────────────────────────
API_KEY    = os.getenv("BINANCE_TESTNET_KEY")
API_SECRET = os.getenv("BINANCE_TESTNET_SECRET")
TESTNET    = True   # ← cambiar a False para mainnet

# ────────────────────────────────────────────────────────────
#  CONTRATO BSC
# ────────────────────────────────────────────────────────────
CONTRACT_ENABLED = True
CONTRACT_ADDRESS = "0x83AE9b980342FAC9F648Ce72d8fCf1E4f584f67c"
BOT_WALLET_PK    = os.getenv("BOT_WALLET_PK")
BSC_RPC          = "https://bsc-dataseed1.binance.org/"
CONTRACT_ABI = [
    {"inputs": [], "name": "depositProfits", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [], "name": "totalInvested",  "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
]

# ────────────────────────────────────────────────────────────
#  PARÁMETROS
# ────────────────────────────────────────────────────────────
PARES          = ['BTC', 'ETH', 'BNB', 'SOL', 'DOGE']   # activos a monitorear
MIN_SPREAD_PCT = 0.15        # spread mínimo Futuros-Spot para entrar (%)
CLOSE_SPREAD   = 0.05        # spread para cerrar (convergencia)
TRADE_USDT     = 45.0        # capital por lado (~45 Spot + ~45 Futuros = ~$90 total)
LEVERAGE       = 2           # apalancamiento en Futuros (conservador)
FEE_SPOT       = 0.001       # 0.1% fee Spot
FEE_FUTURES    = 0.0004      # 0.04% fee Futuros maker
SCAN_SEC       = 0.5         # intervalo de escaneo
COOLDOWN_SEC   = 10.0        # segundos entre operaciones
LOG_FILE       = "bot_cashcarry.log"
WEIGHT_LIMIT   = 6000
WEIGHT_PAUSE   = 0.80

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
log = logging.getLogger("CASHCARRY-v10")

# ────────────────────────────────────────────────────────────
#  ESTADO GLOBAL
# ────────────────────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.running          = True
        self.lock             = threading.Lock()
        self.spot_prices      = {}      # {símbolo: {bid, ask}}
        self.futures_prices   = {}      # {símbolo: {bid, ask}}
        self.posiciones       = {}      # {activo: {spot_qty, futures_qty, spot_precio, futures_precio}}
        self.lot_sizes_spot   = {}
        self.lot_sizes_fut    = {}
        self.total_pnl        = 0.0
        self.profit_pendiente = 0.0
        self.trades           = 0
        self.wins             = 0
        self.losses           = 0
        self.last_trade       = 0
        self.used_weight      = 0
        self.weight_reset     = time.time()

state  = BotState()
client = Client(API_KEY, API_SECRET, testnet=TESTNET)

# ────────────────────────────────────────────────────────────
#  RATE LIMITER
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
        log.warning(f"⚠️  Rate limit {pct*100:.0f}% — pausando {pausa:.1f}s")
        time.sleep(pausa)

# ────────────────────────────────────────────────────────────
#  WEBSOCKET SPOT
# ────────────────────────────────────────────────────────────
def ws_spot_thread():
    asyncio.run(ws_spot_stream())

async def ws_spot_stream():
    streams = "/".join([f"{p.lower()}usdt@bookTicker" for p in PARES])
    ws_host = "testnet.binance.vision" if TESTNET else "stream.binance.com:9443"
    url = f"wss://{ws_host}/stream?streams={streams}"
    log.info(f"📡 WS Spot conectando ({len(PARES)} pares) [{'TESTNET' if TESTNET else 'MAINNET'}]...")
    while state.running:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("✅ WS Spot conectado")
                async for msg in ws:
                    d = json.loads(msg).get("data", {})
                    sym = d.get("s")
                    if sym:
                        with state.lock:
                            state.spot_prices[sym] = {
                                "bid": float(d["b"]),
                                "ask": float(d["a"])
                            }
        except Exception as e:
            log.error(f"WS Spot error: {e} — reconectando en 3s")
            await asyncio.sleep(3)

# ────────────────────────────────────────────────────────────
#  WEBSOCKET FUTUROS
# ────────────────────────────────────────────────────────────
def ws_futures_thread():
    asyncio.run(ws_futures_stream())

async def ws_futures_stream():
    streams = "/".join([f"{p.lower()}usdt@bookTicker" for p in PARES])
    ws_host = "stream.binancefuture.com" if TESTNET else "fstream.binance.com"
    url = f"wss://{ws_host}/stream?streams={streams}"
    log.info(f"📡 WS Futuros conectando ({len(PARES)} pares) [{'TESTNET' if TESTNET else 'MAINNET'}]...")
    while state.running:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("✅ WS Futuros conectado")
                async for msg in ws:
                    d = json.loads(msg).get("data", {})
                    sym = d.get("s")
                    if sym:
                        with state.lock:
                            state.futures_prices[sym] = {
                                "bid": float(d["b"]),
                                "ask": float(d["a"])
                            }
        except Exception as e:
            log.error(f"WS Futuros error: {e} — reconectando en 3s")
            await asyncio.sleep(3)

# ────────────────────────────────────────────────────────────
#  LOT SIZES
# ────────────────────────────────────────────────────────────
def cargar_lot_sizes():
    # Spot
    try:
        info = client.get_exchange_info()
        for s in info["symbols"]:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    state.lot_sizes_spot[s["symbol"]] = float(f["stepSize"])
        log.info(f"✅ LOT_SIZE Spot: {len(state.lot_sizes_spot)} pares")
    except Exception as e:
        log.error(f"Error lot sizes Spot: {e}")

    # Futuros
    try:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    state.lot_sizes_fut[s["symbol"]] = float(f["stepSize"])
        log.info(f"✅ LOT_SIZE Futuros: {len(state.lot_sizes_fut)} pares")
    except Exception as e:
        log.error(f"Error lot sizes Futuros: {e}")

def ajustar_qty(symbol, qty, es_futuro=False):
    sizes = state.lot_sizes_fut if es_futuro else state.lot_sizes_spot
    step  = sizes.get(symbol, 0.00001)
    if step == 0: step = 0.00001
    decimals = len(f"{step:.10f}".rstrip("0").split(".")[-1])
    return round(int(qty / step) * step, decimals)

# ────────────────────────────────────────────────────────────
#  CONFIGURAR APALANCAMIENTO
# ────────────────────────────────────────────────────────────
def configurar_leverage():
    for par in PARES:
        symbol = f"{par}USDT"
        try:
            client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
            log.info(f"✅ Leverage {LEVERAGE}x configurado: {symbol}")
        except Exception as e:
            log.error(f"Error leverage {symbol}: {e}")

# ────────────────────────────────────────────────────────────
#  CALCULAR SPREAD
# ────────────────────────────────────────────────────────────
def calcular_spread(activo: str):
    """
    Retorna spread% = (futures_bid - spot_ask) / spot_ask * 100
    Positivo = Futuros más caro = oportunidad de entrada
    Negativo = Spot más caro = oportunidad de cierre
    """
    symbol = f"{activo}USDT"
    with state.lock:
        spot    = state.spot_prices.get(symbol)
        futures = state.futures_prices.get(symbol)

    if not spot or not futures:
        return None, None, None

    spot_ask    = spot["ask"]
    futures_bid = futures["bid"]

    if spot_ask == 0:
        return None, None, None

    spread_pct = ((futures_bid - spot_ask) / spot_ask) * 100
    return spread_pct, spot_ask, futures_bid

# ────────────────────────────────────────────────────────────
#  ABRIR POSICIÓN (BUY Spot + SHORT Futuros)
# ────────────────────────────────────────────────────────────
def abrir_posicion(activo: str, spot_ask: float, futures_bid: float):
    symbol = f"{activo}USDT"
    qty_spot = ajustar_qty(symbol, TRADE_USDT / spot_ask, es_futuro=False)
    qty_fut  = ajustar_qty(symbol, TRADE_USDT / futures_bid, es_futuro=True)

    if qty_spot <= 0 or qty_fut <= 0:
        log.error(f"❌ Qty inválida: Spot={qty_spot} Fut={qty_fut}")
        return False

    # BUY Spot
    try:
        check_rate_limit(weight=1)
        orden_spot = client.create_order(
            symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=qty_spot
        )
        precio_spot_real = float(orden_spot["fills"][0]["price"]) if orden_spot.get("fills") else spot_ask
        log.info(f"  ✅ BUY Spot {qty_spot} {activo} @ ${precio_spot_real:,.4f}")
    except Exception as e:
        log.error(f"  ❌ Error BUY Spot {symbol}: {e}")
        return False

    # SHORT Futuros
    try:
        check_rate_limit(weight=1)
        orden_fut = client.futures_create_order(
            symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty_fut
        )
        precio_fut_real = float(orden_fut["avgPrice"]) if orden_fut.get("avgPrice") else futures_bid
        log.info(f"  ✅ SHORT Fut {qty_fut} {activo} @ ${precio_fut_real:,.4f}")
    except Exception as e:
        log.error(f"  ❌ Error SHORT Futuros {symbol}: {e}")
        # Si falla el futuro, cerrar el spot para no quedar expuesto
        log.warning(f"  ⚠️  Cerrando Spot por fallo en Futuros...")
        try:
            client.create_order(
                symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty_spot
            )
        except Exception as e2:
            log.error(f"  ❌ Error cerrando Spot emergencia: {e2}")
        return False

    # Registrar posición
    with state.lock:
        state.posiciones[activo] = {
            "spot_qty":       qty_spot,
            "futures_qty":    qty_fut,
            "spot_precio":    precio_spot_real,
            "futures_precio": precio_fut_real,
            "tiempo":         time.time()
        }
        state.last_trade = time.time()
        state.trades    += 1

    spread_entrada = ((precio_fut_real - precio_spot_real) / precio_spot_real) * 100
    log.info(f"📊 Posición abierta {activo} | Spread entrada: {spread_entrada:+.4f}%")
    return True

# ────────────────────────────────────────────────────────────
#  CERRAR POSICIÓN (SELL Spot + CLOSE SHORT Futuros)
# ────────────────────────────────────────────────────────────
def cerrar_posicion(activo: str, motivo: str = "convergencia"):
    with state.lock:
        pos = state.posiciones.get(activo)
    if not pos:
        return

    symbol = f"{activo}USDT"

    # SELL Spot
    try:
        check_rate_limit(weight=1)
        orden_spot = client.create_order(
            symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=pos["spot_qty"]
        )
        precio_spot_cierre = float(orden_spot["fills"][0]["price"]) if orden_spot.get("fills") else 0
        log.info(f"  ✅ SELL Spot {pos['spot_qty']} {activo} @ ${precio_spot_cierre:,.4f}")
    except Exception as e:
        log.error(f"  ❌ Error SELL Spot {symbol}: {e}")
        precio_spot_cierre = 0

    # CLOSE SHORT Futuros (BUY para cerrar)
    try:
        check_rate_limit(weight=1)
        orden_fut = client.futures_create_order(
            symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET,
            quantity=pos["futures_qty"], reduceOnly=True
        )
        precio_fut_cierre = float(orden_fut["avgPrice"]) if orden_fut.get("avgPrice") else 0
        log.info(f"  ✅ CLOSE SHORT {pos['futures_qty']} {activo} @ ${precio_fut_cierre:,.4f}")
    except Exception as e:
        log.error(f"  ❌ Error CLOSE SHORT {symbol}: {e}")
        precio_fut_cierre = 0

    # Calcular P&L
    if precio_spot_cierre > 0 and precio_fut_cierre > 0:
        pnl_spot    = (precio_spot_cierre - pos["spot_precio"])    * pos["spot_qty"]
        pnl_fut     = (pos["futures_precio"] - precio_fut_cierre)  * pos["futures_qty"]
        fee_total   = (TRADE_USDT * FEE_SPOT * 2) + (TRADE_USDT * FEE_FUTURES * 2)
        pnl_neto    = pnl_spot + pnl_fut - fee_total
        duracion    = int(time.time() - pos["tiempo"])

        with state.lock:
            state.total_pnl += pnl_neto
            if pnl_neto >= 0:
                state.wins += 1
            else:
                state.losses += 1
            del state.posiciones[activo]

        log.info(f"{'✅' if pnl_neto >= 0 else '❌'} CERRADO [{motivo}] {activo} | "
                 f"P&L: ${pnl_neto:+.4f} | Spot: ${pnl_spot:+.4f} | Fut: ${pnl_fut:+.4f} | "
                 f"Fees: -${fee_total:.4f} | Duración: {duracion}s | "
                 f"Total P&L: ${state.total_pnl:+.4f}")

        # Depositar ganancia en contrato BSC
        if pnl_neto > 0:
            depositar_ganancia_contrato(pnl_neto)
    else:
        with state.lock:
            del state.posiciones[activo]
        log.warning(f"⚠️  Posición {activo} cerrada [{motivo}] — P&L no calculado")

# ────────────────────────────────────────────────────────────
#  CONTRATO BSC — INIT Y DEPÓSITO
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
        log.info(f"✅ Contrato BSC: {CONTRACT_ADDRESS[:12]}...")
    except Exception as e:
        log.error(f"Error contrato: {e}")

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
        w3      = Web3(Web3.HTTPProvider(BSC_RPC))
        account = w3.eth.account.from_key(BOT_WALLET_PK)
        total   = contract.functions.totalInvested().call()
        if total == 0:
            return
        with state.lock:
            bnb_price = state.spot_prices.get("BNBUSDT", {}).get("bid", 0)
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
            log.info(f"✅ Contrato: ${pendiente:.4f} depositado")
            with state.lock:
                state.profit_pendiente = 0.0
    except Exception as e:
        log.error(f"Error depositando contrato: {e}")

# ────────────────────────────────────────────────────────────
#  MOTOR PRINCIPAL
# ────────────────────────────────────────────────────────────
def arbitrage_engine():
    log.info("🔍 Motor Cash & Carry iniciado")

    # Esperar precios
    for _ in range(30):
        with state.lock:
            n_spot = len(state.spot_prices)
            n_fut  = len(state.futures_prices)
        if n_spot >= len(PARES) and n_fut >= len(PARES):
            break
        time.sleep(1)

    log.info(f"✅ Precios Spot: {len(state.spot_prices)} | Futuros: {len(state.futures_prices)}")

    while state.running:
        try:
            # ── Revisar posiciones abiertas ───────────────────
            with state.lock:
                posiciones_activas = dict(state.posiciones)

            for activo, pos in posiciones_activas.items():
                spread_pct, spot_ask, futures_bid = calcular_spread(activo)
                if spread_pct is None:
                    continue

                # Cerrar si spread convergió o se invirtió
                if spread_pct <= CLOSE_SPREAD:
                    log.info(f"🎯 Spread convergió {activo}: {spread_pct:+.4f}% — cerrando")
                    cerrar_posicion(activo, motivo=f"spread {spread_pct:+.4f}%")

            # ── Buscar nuevas entradas ────────────────────────
            cooldown_ok = (time.time() - state.last_trade) > COOLDOWN_SEC

            if cooldown_ok:
                spreads = []
                for activo in PARES:
                    spread_pct, spot_ask, futures_bid = calcular_spread(activo)
                    if spread_pct is not None:
                        spreads.append((spread_pct, activo, spot_ask, futures_bid))

                spreads.sort(key=lambda x: x[0], reverse=True)

                # Log top spreads
                top_str = " | ".join(
                    f"{s[1]}: {s[0]:+.4f}%" for s in spreads
                )
                with state.lock:
                    pnl = state.total_pnl
                    w   = state.wins
                    l   = state.losses
                    pos_count = len(state.posiciones)

                log.info(f"📊 {top_str} | P&L: ${pnl:+.2f} | W/L: {w}/{l} | Pos: {pos_count}")

                # Entrar en la mejor oportunidad si supera umbral
                for spread_pct, activo, spot_ask, futures_bid in spreads:
                    # No abrir si ya hay posición en ese activo
                    with state.lock:
                        ya_abierto = activo in state.posiciones

                    if ya_abierto:
                        continue

                    if spread_pct >= MIN_SPREAD_PCT:
                        fee_estimada = (FEE_SPOT + FEE_FUTURES) * 2 * 100  # en %
                        ganancia_est = (spread_pct - fee_estimada) * TRADE_USDT / 100
                        log.info(f"🚀 Oportunidad {activo} | Spread: {spread_pct:+.4f}% | "
                                 f"Est neto: ${ganancia_est:+.4f}")
                        abrir_posicion(activo, spot_ask, futures_bid)
                        break  # una operación por ciclo

        except Exception as e:
            log.error(f"Error motor: {e}")

        time.sleep(SCAN_SEC)

# ────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────
def main():
    log.info("═" * 65)
    log.info(f"  BOT CASH & CARRY v10 — {'⚠️  TESTNET' if TESTNET else '🔴 MAINNET'}")
    log.info(f"  Pares      : {', '.join(PARES)}")
    log.info(f"  Capital    : ${TRADE_USDT:.0f} por lado | Leverage: {LEVERAGE}x")
    log.info(f"  Min spread : {MIN_SPREAD_PCT}% | Cierre: {CLOSE_SPREAD}%")
    log.info(f"  Fees       : Spot {FEE_SPOT*100:.1f}% | Fut {FEE_FUTURES*100:.3f}%")
    log.info("═" * 65)

    cargar_lot_sizes()
    configurar_leverage()
    init_contract()

    threading.Thread(target=ws_spot_thread,    daemon=True).start()
    threading.Thread(target=ws_futures_thread, daemon=True).start()
    threading.Thread(target=arbitrage_engine,  daemon=True).start()

    log.info("✅ Bot v10 Cash & Carry iniciado. Ctrl+C para detener.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        state.running = False
        log.info(f"RESUMEN FINAL | W: {state.wins} | L: {state.losses} | P&L: ${state.total_pnl:+.4f}")
        # Cerrar posiciones abiertas al salir
        with state.lock:
            activos_abiertos = list(state.posiciones.keys())
        for activo in activos_abiertos:
            log.info(f"⚠️  Cerrando posición abierta: {activo}")
            cerrar_posicion(activo, motivo="shutdown")

if __name__ == "__main__":
    main()
