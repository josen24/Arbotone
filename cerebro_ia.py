# cerebro_ia.py
# Fase 1: Modo aprendizaje — aprueba todo y registra datos para entrenar después
import time
import csv
import os

HISTORIAL_FILE = "historial_decisiones.csv"
_registro_count = 0

# Inicializar archivo de historial
if not os.path.exists(HISTORIAL_FILE):
    with open(HISTORIAL_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "precio_compra", "precio_venta",
            "volumen", "spread", "spread_pct", "decision"
        ])

print("⚡ Cerebro IA cargado — Fase 1: Modo aprendizaje activo.")

def analizar_oportunidad(precio_compra: float, precio_venta: float, volumen: float) -> bool:
    """
    Fase 1 — Aprueba toda operación que pase el filtro del bot.
    Registra cada decision para entrenar el modelo en Fase 2.
    """
    global _registro_count

    spread = precio_venta - precio_compra
    spread_pct = (spread / precio_compra * 100) if precio_compra > 0 else 0

    # Registrar para aprendizaje futuro
    try:
        with open(HISTORIAL_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                time.time(), precio_compra, precio_venta,
                volumen, spread, round(spread_pct, 6), True
            ])
        _registro_count += 1
        if _registro_count % 100 == 0:
            print(f"🧠 IA: {_registro_count} operaciones registradas para entrenamiento futuro")
    except Exception:
        pass

    # Fase 1: siempre aprobar — dejar que el bot opere libremente
    return True
