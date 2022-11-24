import mysql.connector as msql
from mysql.connector import Error
from functions.private.my_password import my_password #MySQL password

def create_database(database_name:str,host = 'localhost',port=3306,user='root'):
    """Creates MySQL database"""
    
    query = f"CREATE DATABASE {database_name}"
    try:
        conn = msql.connect(host=host, port=port, user=user, password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(query)
            print("Database is created")
    except Error as e:
        print("Error", e)