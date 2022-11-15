import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #contraseña de MySQL

def create_table(database_name:str,table_name:str,variables:str):
    """
        Creates table in database. Drops if exists.
        Variables describes the name and data type for each column in MySQL language.
    """
    query_1 = f'DROP TABLE IF EXISTS {table_name};'
    query_2 = f"CREATE TABLE {table_name}({variables})ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;"
    try: 
        conn = msql.connect(host='localhost', database=database_name, user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conectado a database: ", record)
            #Creo tablas
            cursor.execute(query_1)
            print('Creando tabla....')
            cursor.execute(query_2)
            print("La tabla ha sido creada")
    except Error as e:
        print("Error al conectar con MySQL", e)