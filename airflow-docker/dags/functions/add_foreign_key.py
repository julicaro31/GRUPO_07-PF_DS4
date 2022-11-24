import mysql.connector as msql
from mysql.connector import Error
from functions.private.my_password import my_password #MySQL password

def add_fk(column_name,database_name,table_name,parent_table,constraint,column_name_parent=None,host='localhost',user='root',port=3306):
    """ 
        Adds foreing keys
    """
    
    try: 
        conn = msql.connect(host=host, port=port,database=database_name, user=user, password=my_password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Conecting to database: ", record)
        
        cursor.execute('SET FOREIGN_KEY_CHECKS=0;')
        if not column_name_parent: column_name_parent = column_name
        sql = f'ALTER TABLE {table_name} ADD CONSTRAINT {constraint} FOREIGN KEY ({column_name}) REFERENCES {parent_table}({column_name_parent});'
        print(sql)
        cursor.execute(sql)

        cursor.execute('SET FOREIGN_KEY_CHECKS=1;')
            
        conn.commit()
    except Error as e:
        print("Error", e)