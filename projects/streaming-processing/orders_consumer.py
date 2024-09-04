import json
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables from the .env file
load_dotenv()

# Database connection parameters
db_config = {
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('HOST'),
    'database': os.getenv('DB'),
}

# Kafka configuration parameters
kafka_broker = 'localhost:9093'
kafka_topic = 'order_data'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[kafka_broker],
    api_version=(2, 0, 2),
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Connect to MySQL database with DictCursor
conn = pymysql.connect(**db_config, cursorclass=DictCursor)
cursor = conn.cursor()

def fetch_product_data(product_id):
    query = "SELECT * FROM products WHERE productid = %s"
    cursor.execute(query, (product_id,))
    return cursor.fetchone()

def process_messages():
    messages = []
    logging.info("Starting message processing...")
    for message in consumer:
        product_id = message.value.get('product_id')
        logging.info(f"Processing message with product_id: {product_id}")

        if product_id:
            product_data = fetch_product_data(product_id)
            if product_data:
                logging.info(f"Fetched product data for product_id: {product_id}")
                # Merge product data with the message
                combined_data = {
                    **product_data,      # Product data from DB
                    **message.value      # Message data from Kafka
                }
                messages.append(combined_data)

                # Process and write to file in chunks
                if len(messages) >= 100:  # Adjust chunk size as needed
                    logging.info("Writing chunk of 100 messages to CSV...")
                    write_to_csv(messages)
                    messages = []

    # Write any remaining messages
    if messages:
        logging.info("Writing remaining messages to CSV...")
        write_to_csv(messages)


def write_to_csv(data):
    # Define the path where the CSV will be saved
    csv_path = '/raw/final_output.csv'

    # Convert the data into a DataFrame
    df = pd.DataFrame(data)

    # Check if the file already exists
    file_exists = os.path.isfile(csv_path)

    # Write to CSV, appending if the file exists, and adding the header only if it doesn't exist
    df.to_csv(csv_path, mode='a', header=not file_exists, index=False)
    logging.info(f"Data written to {csv_path}.")


if __name__ == "__main__":
    try:
        logging.info("Starting the consumer...")
        process_messages()
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Closing database connection and Kafka consumer...")
        cursor.close()
        conn.close()
        consumer.close()
        logging.info("Shutdown complete.")
