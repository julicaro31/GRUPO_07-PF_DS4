import mysql.connector as msql
from mysql.connector import Error
from functions.private.my_password import my_password #MySQL password
import pandas as pd

def load_data(dataset_path,database_name,table_name,host='localhost',user='root',port=3306):
    """ 
        Loads data in table.
    """

    df_table = pd.read_csv(dataset_path,dtype=str)
    x = "%s,"*df_table.shape[1]
    x = x.strip(',')
    
    try: 
        conn = msql.connect(host=host,port=port,database=database_name, user=user, password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conecting to database: ", record)
            cursor.execute('SET FOREIGN_KEY_CHECKS=0;')
            print("Loading data...")
            for i,row in df_table.iterrows():
            
                sql = f"INSERT INTO {database_name}.{table_name} VALUES ({x})"
                cursor.execute(sql, tuple(row))
            
                conn.commit()
            cursor.execute('SET FOREIGN_KEY_CHECKS=1;')
    except Error as e:
        print("Error", e)