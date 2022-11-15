import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #contraseña de MySQL
import pandas as pd

def drop_table(database_name,table_name):
    
    """ 
        Drops table.
    """
    
    try: 
        conn = msql.connect(host='localhost', database=database_name, user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conectado a database: ", record)

            sql = f"DROP TABLE {table_name}"
            cursor.execute(sql)
            conn.commit()
            print(f"La tabla {table_name} ha sido eliminada")
    except Error as e:
        print("Error al conectar con MySQL", e)