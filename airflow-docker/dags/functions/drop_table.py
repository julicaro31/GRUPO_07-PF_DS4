import mysql.connector as msql
from mysql.connector import Error
from functions.private.my_password import my_password #MySQL password

def drop_table(database_name,table_name,host='localhost',user='root',port=3306):
    
    """ 
        Drops table.
    """
    
    try: 
        conn = msql.connect(host=host, port=port,database=database_name, user=user, password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conecting to database: ", record)

            sql = f"DROP TABLE {table_name}"
            cursor.execute(sql)
            conn.commit()
            print(f"Table {table_name} has been dropped")
    except Error as e:
        print("Error", e)