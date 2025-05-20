import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def cargar_csv(archivo, separador=None):
    """Carga un archivo CSV detectando automáticamente el delimitador si no se especifica."""
    if separador:
        return pd.read_csv(archivo, sep=separador)
    else:
        with open(archivo, 'r') as f:
            primera_linea = f.readline()
            separador = ';' if primera_linea.count(';') > primera_linea.count(',') else ','
        return pd.read_csv(archivo, sep=separador)

def obtener_datos():
    print("Iniciando la extracción de datos...")
    pedidos_catalogo = cargar_csv('data/Catalog_Orders.csv', separador=';')
    pedidos_web = pd.read_json('data/Web_Orders.json')
    productos = cargar_csv('data/Products.csv', separador=';')
    return pedidos_catalogo, pedidos_web, productos


def transformar_datos(pedidos_catalogo, pedidos_web, productos):
    print("Transformando los datos...")

    columnas_orden = ['order_id', 'order_date', 'customer_id', 'product_id', 'quantity', 'channel']

    # Estandarizar formato de fechas
    for df in [pedidos_catalogo, pedidos_web]:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    # Asignar canal de ventas
    pedidos_catalogo['channel'] = 'Catalogo'
    pedidos_web['channel'] = 'Web'

    # Limpieza de valores nulos
    for df in [pedidos_catalogo, pedidos_web]:
        df.replace(['', 'NA', 'null'], np.nan, inplace=True)
        df.dropna(subset=['product_id', 'quantity'], inplace=True)

    # Seleccionar columnas y unir datasets
    pedidos_catalogo = pedidos_catalogo[columnas_orden]
    pedidos_web = pedidos_web[columnas_orden]
    ventas_unidas = pd.concat([pedidos_catalogo, pedidos_web], ignore_index=True)

    # Limpiar datos de productos
    productos.drop_duplicates(subset='product_id', inplace=True)
    productos.dropna(subset=['product_id', 'product_name'], inplace=True)

    return ventas_unidas, productos

def cargar_a_base_datos(ventas, productos):
    print("Cargando los datos a la base de datos...")

    # Cambia esta cadena de conexión según tu configuración
    motor = create_engine('postgresql://usuario:password@localhost:5432/mi_base')

    ventas.to_sql('ventas', motor, if_exists='replace', index=False)
    productos.to_sql('productos', motor, if_exists='replace', index=False)

    print("Datos cargados correctamente.")

def ejecutar_pipeline():
    print("🚀 Iniciando el pipeline ETL...")
    pedidos_catalogo, pedidos_web, productos = obtener_datos()
    ventas_unidas, productos_limpios = transformar_datos(pedidos_catalogo, pedidos_web, productos)
    cargar_a_base_datos(ventas_unidas, productos_limpios)
    print("🏁 Proceso ETL finalizado.")

if __name__ == '__main__':
    ejecutar_pipeline()


