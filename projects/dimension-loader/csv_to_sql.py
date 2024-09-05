import os
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

# Database connection parameters
DATABASE_HOST = 'localhost'  # Your database host
DATABASE_USER = 'root'  # Your database username
DATABASE_PASSWORD = 'qwerty'  # Your database password
DATABASE_NAME = 'retaildb'  # Your database name

# Folder path for CSV files
CSV_FOLDER_PATH = '../../dataset/dimensions/'

# Create a database connection
connection = pymysql.connect(
    host=DATABASE_HOST,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    database=DATABASE_NAME,
    cursorclass=DictCursor
)

# Define table schemas and expected columns
tables = {
    'employees': {
        'schema': """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INT AUTO_INCREMENT PRIMARY KEY,
            employee_name VARCHAR(255) NOT NULL,
            experience INT,
            salary DECIMAL(10, 2)
        );
        """,
        'columns': ['employee_id', 'employee_name', 'experience', 'salary']
    },
    'categories': {
        'schema': """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INT AUTO_INCREMENT PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL
        );
        """,
        'columns': ['category_id', 'category_name']
    },
    'customers': {
        'schema': """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            dob DATE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            gender ENUM('Male', 'Female', 'Other') NOT NULL,
            country VARCHAR(100) NOT NULL,
            region VARCHAR(100) NOT NULL,
            city VARCHAR(100) NOT NULL,
            asset DECIMAL(15, 2),
            marital_status ENUM('Single', 'Married', 'Divorced', 'Widowed')
        );
        """,
        'columns': ['customer_id', 'name', 'dob', 'email', 'gender', 'country', 'region', 'city', 'asset',
                    'marital_status']
    },
    'products': {
        'schema': """
        CREATE TABLE IF NOT EXISTS products (
            productid INT AUTO_INCREMENT PRIMARY KEY,
            productname VARCHAR(255) NOT NULL,
            categoryid INT NOT NULL,
            price DECIMAL(10, 2) NOT NULL
        );
        """,
        'columns': ['productid', 'productname', 'categoryid', 'price']
    }
}


# Check if the table exists
def table_exists(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None


# Create tables if they don't exist
for table_name, table_info in tables.items():
    if not table_exists(connection, table_name):
        with connection.cursor() as cursor:
            cursor.execute(table_info['schema'])
            connection.commit()
            print(f"Table '{table_name}' created.")
    else:
        print(f"Table '{table_name}' already exists.")

# Import CSV data into the tables
print("------------")

# Loop through each entry in the CSV folder
for directory in os.listdir(CSV_FOLDER_PATH):
    dir_path = os.path.join(CSV_FOLDER_PATH, directory)
    if os.path.isdir(dir_path):
        # List files in the subdirectory
        for file_name in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file_name)

            # Determine table name from the directory name
            table_name = directory

            if table_name in tables:
                try:
                    # Read the CSV file with pipe delimiter and handle quotes
                    df = pd.read_csv(file_path, delimiter='|', quotechar='"', engine='python')

                    # Debug: Print raw column names
                    print(f"Raw columns in '{file_name}': {df.columns.tolist()}")

                    # Normalize column names - strip extra characters
                    df.columns = [col.strip().replace('|', '').replace('"', '') for col in df.columns]
                    expected_columns = [col.lower() for col in tables[table_name]['columns']]

                    # Debug: Print normalized column names
                    print(f"Normalized columns in '{file_name}': {df.columns.tolist()}")

                    # Check if DataFrame columns match the expected columns for the table
                    if all(col in df.columns for col in expected_columns):
                        with connection.cursor() as cursor:
                            for _, row in df.iterrows():
                                # Prepare SQL insert statement
                                columns = ', '.join(expected_columns)
                                values_placeholders = ', '.join(['%s'] * len(expected_columns))
                                update_placeholders = ', '.join([f"{col}=VALUES({col})" for col in expected_columns])
                                sql = f"""
                                INSERT INTO {table_name} ({columns})
                                VALUES ({values_placeholders})
                                ON DUPLICATE KEY UPDATE {update_placeholders};
                                """
                                cursor.execute(sql, tuple(row[col] for col in expected_columns))
                        connection.commit()
                        print(f"Data from '{file_name}' imported successfully.")
                    else:
                        missing_cols = [col for col in expected_columns if col not in df.columns]
                        print(f"'{file_name}' is missing columns: {missing_cols}")
                except Exception as e:
                    print(f"Error importing data from '{file_name}': {e}")

# Close the connection
connection.close()