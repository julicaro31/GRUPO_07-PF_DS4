import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #contraseña de MySQL
import pandas as pd

def load_data(dataset_path,database_name,table_name):
    """ 
        Loads data in table.
    """

    df_tabla = pd.read_csv(dataset_path,dtype=str)
    x = "%s,"*df_tabla.shape[1]
    x = x.strip(',')
    
    try: 
        conn = msql.connect(host='localhost', database=database_name, user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conectado a database: ", record)
        print("Insertando datos...")
        for i,row in df_tabla.iterrows():
            
            sql = f"INSERT INTO {database_name}.{table_name} VALUES ({x})"
            cursor.execute(sql, tuple(row))
            
            conn.commit()
    except Error as e:
        print("Error al conectar con MySQL", e)