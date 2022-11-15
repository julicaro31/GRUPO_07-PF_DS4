import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #contraseña de MySQL

def create_database(database_name:str):
    """Creates MySQL database"""
    
    query = f"CREATE DATABASE {database_name}"
    try:
        conn = msql.connect(host='localhost', user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(query)
            print("La base de datos ha sido creada")
    except Error as e:
        print("Error al conectar con MySQL", e)