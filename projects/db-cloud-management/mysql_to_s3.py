import pymysql
import boto3
import pandas as pd
from io import StringIO
import configparser

# Create a ConfigParser object
config = configparser.ConfigParser()

# Read the properties file
config.read('config.properties')

# MySQL connection details
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'root'
mysql_database = 'retail_db'

# S3 settings
s3_bucket = config.get("BUCKET_NAME")

# Initialize S3 client
s3_client = boto3.client('s3')


def fetch_table_to_csv(table_name):
    connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    try:
        query = f"SELECT * FROM {table_name}"
        with connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            # Get column names
            columns = [desc[0] for desc in cursor.description]
        # Convert to DataFrame
        df = pd.DataFrame(data, columns=columns)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue()
    finally:
        connection.close()


def upload_to_s3(file_content, object_name):
    """Upload a file to S3."""
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=object_name,
        Body=file_content,
        ContentType='text/csv'
    )
    print(f"Uploaded {object_name} to S3 bucket {s3_bucket}")


def main():
    tables = ['employees', 'customers', 'categories','products']
    for table in tables:
        csv_content = fetch_table_to_csv(table)
        s3_object_name = f"{table}/{table}.csv"
        upload_to_s3(csv_content, s3_object_name)


if __name__ == '__main__':
    main()