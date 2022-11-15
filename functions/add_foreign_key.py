import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #contraseña de MySQL
import pandas as pd

def add_fk(column_name,database_name,table_name,parent_table):
    """ 
        Adds foreing keys
    """
    
    try: 
        conn = msql.connect(host='localhost', database=database_name, user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conectado a database: ", record)
        
        cursor.execute('SET FOREIGN_KEY_CHECKS=0;')

        sql = f'ALTER TABLE {table_name} ADD CONSTRAINT foreign_{column_name} FOREIGN KEY ({column_name}) REFERENCES {parent_table}({column_name});'
        print(sql)
        cursor.execute(sql)

        cursor.execute('SET FOREIGN_KEY_CHECKS=1;')
            
        conn.commit()
    except Error as e:
        print("Error al conectar con MySQL", e)