# import json
# import pandas as pd
# import pymysql
# from pymysql.cursors import DictCursor
# from kafka import KafkaConsumer
# import os
# import logging
# import time
#
# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("process.log"),
#         logging.StreamHandler()
#     ]
# )
#
# # Database connection parameters
# db_config = {
#     'user': "root",
#     'password': "root",
#     'host': "localhost",
#     'database': "retail_db",
# }
#
# # Kafka configuration parameters
# kafka_broker = 'localhost:9092'
# kafka_topic = 'retail-topic'
#
# # Initialize Kafka consumer
# consumer = KafkaConsumer(
#     kafka_topic,
#     bootstrap_servers=[kafka_broker],
#     api_version=(2, 0, 2),
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     auto_offset_reset='earliest',  # Start from the beginning of the topic
#     enable_auto_commit=False  # Disable auto commit to manually manage offsets
# )
#
# # Connect to MySQL database with DictCursor
# conn = pymysql.connect(**db_config, cursorclass=DictCursor)
# cursor = conn.cursor()
#
#
# def fetch_product_data(product_id):
#     query = "SELECT * FROM products WHERE productid = %s"
#     cursor.execute(query, (product_id,))
#     result = cursor.fetchone()
#     if result:
#         # Ensure the key is 'productid' for consistency
#         result['productid'] = result.pop('productid')
#     return result
#
#
# def process_messages():
#     messages = []
#     batch_size = 20  # Define the batch size
#     batch_counter = 1  # Initialize batch counter
#     logging.info("Starting message processing...")
#
#     while True:
#         # Poll messages from Kafka
#         records = consumer.poll(timeout_ms=1000)  # Adjust timeout as needed
#
#         for partition, messages_batch in records.items():
#             for message in messages_batch:
#                 product_id = message.value.get('product_id')
#                 logging.info(f"Processing message with product_id: {product_id}")
#
#                 if product_id:
#                     product_data = fetch_product_data(product_id)
#                     if product_data:
#                         logging.info(f"Fetched product data for product_id: {product_id}")
#
#                         # Combine product data with the Kafka message
#                         combined_data = {
#                             **product_data,  # Product data from DB
#                             **message.value  # Message data from Kafka
#                         }
#
#                         # Ensure no duplicate product ID fields
#                         combined_data.pop('product_id', None)
#
#                         # Add to messages
#                         messages.append(combined_data)
#
#                         # Process and write to file in chunks
#                         if len(messages) >= batch_size:
#                             logging.info(f"Writing batch {batch_counter} of {batch_size} messages to CSV...")
#                             write_to_csv(messages, batch_counter)
#                             messages = []
#                             batch_counter += 1
#
#         # Write any remaining messages
#         if messages:
#             logging.info(f"Writing remaining batch {batch_counter} messages to CSV...")
#             write_to_csv(messages, batch_counter)
#             messages = []
#             batch_counter += 1
#
#
# def write_to_csv(data, batch_number):
#     # Define the path where the CSV will be saved
#     timestamp = time.strftime("%Y%m%d_%H%M%S")
#     csv_path = f'../../dataset/processed/batch_{batch_number}_{timestamp}.csv'
#
#     # Convert the data into a DataFrame
#     df = pd.DataFrame(data)
#
#     # Ensure columns are in the desired order
#     desired_columns = [
#         'productname', 'categoryid', 'price', 'productid',
#         'order_id', 'order_date', 'employee_id',
#         'customer_id', 'quantity_sold'
#     ]
#
#     # Reorder columns if necessary
#     df = df[desired_columns]
#
#     # Write to CSV
#     df.to_csv(csv_path, index=False)
#     logging.info(f"Data written to {csv_path}.")
#
#
# if __name__ == "__main__":
#     try:
#         logging.info("Starting the consumer...")
#         process_messages()
#     except KeyboardInterrupt:
#         logging.info("Shutting down consumer...")
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#     finally:
#         logging.info("Closing database connection and Kafka consumer...")
#         cursor.close()
#         conn.close()
#         consumer.close()
#         logging.info("Shutdown complete.")
import json
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
from kafka import KafkaConsumer
import os
import logging
import time
import errno

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process.log"),
        logging.StreamHandler()
    ]
)

# Database connection parameters
db_config = {
    'user': "root",
    'password': "qwerty",
    'host': "localhost",
    'database': "retaildb",
}

# Kafka configuration parameters
kafka_broker = 'localhost:9092'
kafka_topic = 'retail-topic'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[kafka_broker],
    api_version=(2, 0, 2),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    enable_auto_commit=False  # Disable auto commit to manually manage offsets
)

# Connect to MySQL database with DictCursor
conn = pymysql.connect(**db_config, cursorclass=DictCursor)
cursor = conn.cursor()


def fetch_product_data(product_id):
    query = "SELECT * FROM products WHERE productid = %s"
    cursor.execute(query, (product_id,))
    result = cursor.fetchone()
    if result:
        # Ensure the key is 'productid' for consistency
        result['productid'] = result.pop('productid')
    return result


def process_messages():
    messages = []
    batch_size = 10  # Define the batch size
    batch_counter = 1  # Initialize batch counter
    logging.info("Starting message processing...")

    while True:
        # Poll messages from Kafka
        records = consumer.poll(timeout_ms=1000)  # Adjust timeout as needed

        for partition, messages_batch in records.items():
            for message in messages_batch:
                product_id = message.value.get('product_id')
                logging.info(f"Processing message with product_id: {product_id}")

                if product_id:
                    product_data = fetch_product_data(product_id)
                    if product_data:
                        logging.info(f"Fetched product data for product_id: {product_id}")

                        # Combine product data with the Kafka message
                        combined_data = {
                            **product_data,  # Product data from DB
                            **message.value  # Message data from Kafka
                        }

                        # Ensure no duplicate product ID fields
                        combined_data.pop('product_id', None)

                        # Add to messages
                        messages.append(combined_data)

                        # Process and write to file in chunks
                        if len(messages) >= batch_size:
                            logging.info(f"Batch {batch_counter} with {batch_size} messages is ready for processing.")
                            logging.info(f"Starting to write batch {batch_counter} to CSV...")
                            write_to_csv(messages, batch_counter)
                            messages = []
                            batch_counter += 1

        # Write any remaining messages if the script is stopped
        if messages:
            logging.info(f"Batch {batch_counter} with {len(messages)} remaining messages is ready for processing.")
            logging.info(f"Starting to write remaining batch {batch_counter} to CSV...")
            write_to_csv(messages, batch_counter)
            messages = []
            batch_counter += 1


def write_to_csv(data, batch_number):
    timestamp = int(time.time())
    base_path = '../../dataset/processed'
    temp_filename = f'{timestamp}_temp.csv'
    final_filename = f'{timestamp}.csv'

    temp_path = os.path.join(base_path, temp_filename)
    final_path = os.path.join(base_path, final_filename)

    # Ensure unique filenames for temporary and final files
    if os.path.isfile(temp_path):
        os.remove(temp_path)

    # Convert the data into a DataFrame
    df = pd.DataFrame(data)

    # Ensure columns are in the desired order
    desired_columns = [
        'productid', 'productname', 'categoryid', 'price',
        'order_id', 'order_date', 'employee_id',
        'customer_id', 'quantity_sold'
    ]

    # Reorder columns if necessary
    df = df[desired_columns]

    try:
        # Write to a temporary CSV file
        df.to_csv(temp_path, index=False)
        logging.info(f"Data written to temporary file {temp_path}.")

        # Rename temporary file to final filename
        if os.path.isfile(final_path):
            os.remove(final_path)
        os.rename(temp_path, final_path)
        logging.info(f"Temporary file {temp_path} renamed to final file {final_path}.")

    except OSError as e:
        if e.errno == errno.EEXIST:
            logging.error(f"File already exists error: {e}")
        else:
            logging.error(f"Error during file operation: {e}")


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
