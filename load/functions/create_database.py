import mysql.connector as msql
from mysql.connector import Error
from private.my_password import my_password #MySQL password

def create_database(database_name:str,port=3306):
    """Creates MySQL database"""
    
    query = f"CREATE DATABASE {database_name}"
    try:
        conn = msql.connect(host='localhost', port=port, user='root', password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(query)
            print("Database is created")
    except Error as e:
        print("Error", e)