"""
Este script carga datos desde un archivo CSV y los inserta en una base de datos MySQL.
"""

import pandas as pd
import mysql.connector

# 1. Leer archivo CSV con codificación adecuada
df = pd.read_csv(
    r'C:\Users\rrs23\OneDrive\Documentos\OneDrive\Documentos\Datos\carga_inicial.csv',
    encoding='ISO-8859-1'  # Usa 'utf-8-sig' o 'latin1' si es necesario
)

# Convertir la columna 'FECHA' al formato de fecha de MySQL (YYYY-MM-DD)
df['FECHA'] = pd.to_datetime(
    df['FECHA'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

# Limpiar la columna 'PRECIO DE TRABAJO' para eliminar el signo '$' y las comas
df['PRECIO DE TRABAJO'] = df['PRECIO DE TRABAJO'].replace(
    {r'\$': '', ',': ''}, regex=True).astype(float)

# Mostrar un vistazo de los datos cargados
print(f"El archivo tiene {df.shape[0]} filas y {df.shape[1]} columnas.")
# Muestra las primeras filas para verificar que se cargaron correctamente
print(df.head())

# 2. Conectar a la base de datos MySQL
conn = mysql.connector.connect(
    host="localhost",  # Cambia 'localhost' si tu MySQL está en otro servidor
    user="root",  # Tu nombre de usuario de MySQL
    password="Dragon2307*",  # Tu contraseña de MySQL
    database="practicando_sql"  # La base de datos donde tienes la tabla 'practicando_sql'
)

cursor = conn.cursor()

# 3. Crear una lista de tuplas con los datos a insertar
data_to_insert = []
for i, row in df.iterrows():
    data_to_insert.append((
        row['REGIONAL'], row['CIUDAD'], row['CODTRABAJO'], row['FECHA'],
        row['CUENTA'], row['ORDEN'], row['CANTIDAD'], row['NOMBRE TECNICO REAL'],
        row['CEDULA TECNICO REAL'], row['TIPO_TRABAJO'], row['PRECIO DE TRABAJO'],
        row['TIPO DE MOVIL'], row['ESQUEMA DE TRABAJO'], row['COORDINADOR']
    ))

# 4. Insertar los datos de una sola vez usando 'executemany' para mejorar el rendimiento
INSERT_QUERY = """
    INSERT INTO practicando_sql (REGIONAL, CIUDAD, CODTRABAJO, FECHA, CUENTA, ORDEN, CANTIDAD, 
                                 `NOMBRE TECNICO REAL`, `CEDULA TECNICO REAL`, TIPO_TRABAJO, 
                                 `PRECIO DE TRABAJO`, `TIPO DE MOVIL`, `ESQUEMA DE TRABAJO`, COORDINADOR)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Usar executemany para insertar las filas de manera más eficiente
cursor.executemany(INSERT_QUERY, data_to_insert)

# Confirmar los cambios
conn.commit()

# 5. Cerrar la conexión
cursor.close()
conn.close()

print("Los datos han sido insertados exitosamente.")
