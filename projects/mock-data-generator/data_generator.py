import pandas as pd
from datetime import datetime, timedelta
import random
import time
import os
import configparser
from orders_state_file_utility import OrdersStateFileUtility


class OrderDataGenerator:
    def __init__(self, config_file='config.properties'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Fetch ranges from the config file
        self.employee_ids = self.get_range('employee_id_range')
        self.customer_ids = self.get_range('customer_id_range')
        self.product_ids = self.get_range('product_id_range')
        self.quantity_range = self.get_range('quantity_range')

        # Dates and other configurations
        self.start_date = datetime.strptime(self.config['DEFAULT']['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.config['DEFAULT']['end_date'], '%Y-%m-%d')
        self.csv_directory = self.config['DEFAULT']['csv_directory']
        self.interval_seconds = int(self.config['DEFAULT']['interval_seconds'])

        if not os.path.exists(self.csv_directory):
            os.makedirs(self.csv_directory)

    def get_range(self, key):
        """Helper function to get a range from config string"""
        range_str = self.config['DEFAULT'][key].split(",")
        return range(int(range_str[0]), int(range_str[1]) + 1)

    def random_date(self, start, end):
        """Generate a random date between start and end"""
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    def generate_data(self):
        """Generate order data, including the introduction of None values"""
        self.order_id_start = OrdersStateFileUtility.read_last_order_id()
        self.order_id_end = self.order_id_start + int(self.config['DEFAULT']['order_id_end'])

        OrdersStateFileUtility.update_last_order_id(self.order_id_end)

        def random_choice(choices):
            """Return an integer from choices or None with a 10% chance"""
            return random.choice(choices)
        def nullable_random_choice(choices):
            """Return an integer from choices or None with a 10% chance"""
            return random.choice(choices) if random.randint(1, 10) > 1 else None

        return {
            "order_id": list(range(self.order_id_start, self.order_id_end)),
            "order_date": [self.random_date(self.start_date, self.end_date) for _ in range(self.order_id_end - self.order_id_start)],
            "employee_id": [random_choice(self.employee_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "product_id": [random_choice(self.product_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "customer_id": [random_choice(self.customer_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "quantity_sold": [nullable_random_choice(self.quantity_range) for _ in range(self.order_id_end - self.order_id_start)],
        }

    def save_to_csv(self):
        """Save the generated data to a CSV file"""
        df = pd.DataFrame(self.generate_data())

        # Ensure 'None' values are not converted to floats
        df = df.astype({
            "order_id": "Int64",
            "employee_id": "Int64",
            "product_id": "Int64",
            "customer_id": "Int64",
            "quantity_sold": "Int64"
        })

        df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')
        epoch_time = int(time.time())

        # Generate filenames
        processing_filename = f"{epoch_time}_processing.csv"
        final_filename = f"{epoch_time}.csv"

        # Paths
        processing_csv_path = os.path.join(self.csv_directory, processing_filename)
        final_csv_path = os.path.join(self.csv_directory, final_filename)

        # Save the DataFrame to a file with "_processing"
        df.to_csv(processing_csv_path, index=False)
        time.sleep(1)  # Short delay to ensure the file is fully written and closed

        # Rename the file to remove "_processing"
        os.rename(processing_csv_path, final_csv_path)

    def run(self):
        """Continuously generate and save CSV files at specified intervals"""
        while True:
            self.save_to_csv()
            time.sleep(self.interval_seconds)
