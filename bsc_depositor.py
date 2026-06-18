"""
bsc_depositor.py
Monitor del log del HedgeBot_Crash1000 → deposita ganancias al contrato BSC
Corre en paralelo con MetaTrader 5
"""

import time
import re
import os
import logging
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ────────────────────────────────────────────────────────────
LOG_DIR          = r"C:\Users\Polan\AppData\Roaming\MetaQuotes\Terminal\D0E8209F77C8CF37AD8BF550E51FF075\MQL5\Logs"
CHECK_SEC        = 5          # cada cuántos segundos revisa el log
MIN_DEPOSITO     = 1.0        # mínimo en USD para depositar
BSC_RPC          = "https://bsc-dataseed1.binance.org/"
CONTRACT_ADDRESS = "0x83AE9b980342FAC9F648Ce72d8fCf1E4f584f67c"
BOT_WALLET_PK    = os.getenv("BOT_WALLET_PK")
CONTRACT_ABI     = [
    {"inputs": [], "name": "depositProfits", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [], "name": "totalInvested",  "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
]

# ────────────────────────────────────────────────────────────
#  LOGGING
# ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("bsc_depositor.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("BSC-Depositor")

# ────────────────────────────────────────────────────────────
#  WEB3 SETUP
# ────────────────────────────────────────────────────────────
def init_web3():
    try:
        w3 = Web3(Web3.HTTPProvider(BSC_RPC))
        if not w3.is_connected():
            log.error("❌ No se pudo conectar a BSC")
            return None, None
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=CONTRACT_ABI
        )
        log.info(f"✅ Conectado a BSC | Contrato: {CONTRACT_ADDRESS[:12]}...")
        return w3, contract
    except Exception as e:
        log.error(f"Error iniciando Web3: {e}")
        return None, None

# ────────────────────────────────────────────────────────────
#  DEPOSITAR EN CONTRATO
# ────────────────────────────────────────────────────────────
def depositar(w3, contract, ganancia_usdt: float, bnb_precio: float):
    try:
        account = w3.eth.account.from_key(BOT_WALLET_PK)

        # Verificar que hay inversores
        total = contract.functions.totalInvested().call()
        if total == 0:
            log.warning("⚠️  No hay inversores en el contrato — depósito omitido")
            return False

        # Convertir USD a BNB
        ganancia_bnb = ganancia_usdt / bnb_precio
        ganancia_wei = w3.to_wei(ganancia_bnb, "ether")

        # Verificar saldo BNB
        saldo = w3.eth.get_balance(account.address)
        if saldo < ganancia_wei + w3.to_wei(0.001, "ether"):  # reserva para gas
            log.error(f"❌ Saldo BNB insuficiente: {w3.from_wei(saldo, 'ether'):.6f} BNB")
            return False

        nonce = w3.eth.get_transaction_count(account.address)
        tx = contract.functions.depositProfits().build_transaction({
            "from":     account.address,
            "value":    ganancia_wei,
            "gas":      200000,
            "gasPrice": w3.to_wei("5", "gwei"),
            "nonce":    nonce,
            "chainId":  56
        })
        signed  = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

        if receipt["status"] == 1:
            log.info(f"✅ Depositado ${ganancia_usdt:.4f} ({ganancia_bnb:.6f} BNB) | TX: {tx_hash.hex()[:16]}...")
            return True
        else:
            log.error(f"❌ TX fallida: {tx_hash.hex()}")
            return False

    except Exception as e:
        log.error(f"Error depositando: {e}")
        return False

# ────────────────────────────────────────────────────────────
#  OBTENER PRECIO BNB
# ────────────────────────────────────────────────────────────
def obtener_precio_bnb():
    try:
        import urllib.request
        import json
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT"
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            return float(data["price"])
    except Exception as e:
        log.error(f"Error obteniendo precio BNB: {e}")
        return 0.0

# ────────────────────────────────────────────────────────────
#  LEER LOG DE HEDGEBOT
# ────────────────────────────────────────────────────────────
def obtener_log_actual():
    """Obtiene el archivo de log más reciente del día en la carpeta MT5."""
    try:
        archivos = [
            f for f in os.listdir(LOG_DIR)
            if f.endswith(".log")
        ]
        if not archivos:
            return None
        # El log del día tiene formato YYYYMMDD.log — el más reciente es el mayor
        archivos.sort(reverse=True)
        return os.path.join(LOG_DIR, archivos[0])
    except Exception as e:
        log.error(f"Error buscando log: {e}")
        return None

def leer_ganancias_log(ultimo_pos: int):
    """
    Lee el log de HedgeBot desde la última posición y extrae ganancias.
    Busca líneas como: ✅ CERRADO [crash spike] | P&L: $12.34
    """
    ganancia_total = 0.0
    nueva_pos      = ultimo_pos

    LOG_FILE = obtener_log_actual()
    if not LOG_FILE or not os.path.exists(LOG_FILE):
        return 0.0, ultimo_pos

    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(ultimo_pos)
            lineas    = f.readlines()
            nueva_pos = f.tell()

        patron = re.compile(r"✅ CERRADO .+\| P&L: \$([0-9]+\.[0-9]+)")
        for linea in lineas:
            match = patron.search(linea)
            if match:
                pnl = float(match.group(1))
                ganancia_total += pnl
                log.info(f"💰 Ganancia detectada en log: ${pnl:.4f}")

    except Exception as e:
        log.error(f"Error leyendo log: {e}")

    return ganancia_total, nueva_pos

# ────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────
def main():
    log.info("═" * 55)
    log.info("  BSC DEPOSITOR — Monitor HedgeBot Crash 1000")
    log.info(f"  Log dir MT5  : {LOG_DIR}")
    log.info(f"  Contrato     : {CONTRACT_ADDRESS[:12]}...")
    log.info(f"  Min depósito : ${MIN_DEPOSITO:.2f}")
    log.info(f"  Check cada   : {CHECK_SEC}s")
    log.info("═" * 55)

    if not BOT_WALLET_PK:
        log.error("❌ BOT_WALLET_PK no encontrada en .env — abortando")
        return

    w3, contract = init_web3()
    if not w3:
        log.error("❌ No se pudo iniciar Web3 — abortando")
        return

    ultimo_pos        = 0
    ganancia_acum     = 0.0

    # Empezar desde el final del log actual
    log_actual = obtener_log_actual()
    if log_actual and os.path.exists(log_actual):
        with open(log_actual, "rb") as f:
            f.seek(0, 2)
            ultimo_pos = f.tell()
        log.info(f"📄 Log encontrado: {os.path.basename(log_actual)} — monitoreando desde posición {ultimo_pos}")
    else:
        log.warning(f"⚠️  No se encontró log en: {LOG_DIR}")

    log.info("✅ Monitor iniciado. Ctrl+C para detener.\n")

    try:
        while True:
            # Leer nuevas ganancias del log
            ganancia_nueva, ultimo_pos = leer_ganancias_log(ultimo_pos)
            ganancia_acum += ganancia_nueva

            if ganancia_acum >= MIN_DEPOSITO:
                log.info(f"🚀 Ganancia acumulada ${ganancia_acum:.4f} — depositando en contrato...")
                bnb_precio = obtener_precio_bnb()
                if bnb_precio > 0:
                    exito = depositar(w3, contract, ganancia_acum, bnb_precio)
                    if exito:
                        ganancia_acum = 0.0
                else:
                    log.warning("⚠️  No se pudo obtener precio BNB — reintentando en próximo ciclo")
            else:
                if ganancia_acum > 0:
                    log.info(f"⏳ Acumulado: ${ganancia_acum:.4f} / ${MIN_DEPOSITO:.2f} para depositar")

            time.sleep(CHECK_SEC)

    except KeyboardInterrupt:
        log.info(f"\nDetenido | Ganancia acumulada sin depositar: ${ganancia_acum:.4f}")

if __name__ == "__main__":
    main()
