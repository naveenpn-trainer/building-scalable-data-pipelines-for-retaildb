import pymysql

# Database connection parameters
config = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'retail_db',
}

# Connect to database
conn = pymysql.connect(**config)
cursor = conn.cursor()

# Load the MySQL script from a file
with open('retail_db.sql', 'r') as file:
    sql_script = file.read()


try:
    for statement in sql_script.split(';'):
        if statement.strip():
            cursor.execute(statement)
    conn.commit()
except pymysql.MySQLError as err:
    print(f"Error: {err}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
