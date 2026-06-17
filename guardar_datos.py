# guardar_datos.py
import csv
import os
import time

ARCHIVO_DATOS = "historial_mercado_binance.csv"

def inicializar_base_de_datos():
    """Crea el archivo CSV con los encabezados si aún no existe."""
    if not os.path.exists(ARCHIVO_DATOS):
        with open(ARCHIVO_DATOS, mode='w', newline='') as f:
            escritor = csv.writer(f)
            escritor.writerow([
                "timestamp_unix",   # Tiempo exacto en milisegundos
                "precio_compra",    # Bid price
                "precio_venta",     # Ask price
                "volumen_compra",   # Bid quantity
                "volumen_venta",    # Ask quantity
                "spread_bruto"      # Diferencia directa de precio
            ])
        print(f"📊 Archivo de datos creado correctamente: {ARCHIVO_DATOS}")

def registrar_evento_mercado(compra: float, venta: float, vol_compra: float, vol_venta: float):
    """Guarda una nueva fila de datos en el archivo CSV en microsegundos."""
    spread = venta - compra
    timestamp = time.time()
    
    with open(ARCHIVO_DATOS, mode='a', newline='') as f:
        escritor = csv.writer(f)
        escritor.writerow([timestamp, compra, venta, vol_compra, vol_venta, spread])

inicializar_base_de_datos()
