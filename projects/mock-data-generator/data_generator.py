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
        self.employee_ids = range(int(self.config['DEFAULT']['employee_id_range'].split(",")[0]),
                                  int(self.config['DEFAULT']['employee_id_range'].split(",")[1]))
        self.customer_ids = range(int(self.config['DEFAULT']['customer_id_start']),
                                  int(self.config['DEFAULT']['customer_id_end']) + 1)
        self.product_ids = range(int(self.config['DEFAULT']['product_id_start']),
                                 int(self.config['DEFAULT']['product_id_end']) + 1)
        self.quantity_range = range(int(self.config['DEFAULT']['quantity_min']),
                                    int(self.config['DEFAULT']['quantity_max']) + 1)

        self.start_date = datetime.strptime(self.config['DEFAULT']['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.config['DEFAULT']['end_date'], '%Y-%m-%d')
        self.csv_directory = self.config['DEFAULT']['csv_directory']
        self.interval_seconds = int(self.config['DEFAULT']['interval_seconds'])

        if not os.path.exists(self.csv_directory):
            os.makedirs(self.csv_directory)

    def random_date(self, start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    def generate_data(self):
        self.order_id_start = OrdersStateFileUtility.read_last_order_id()
        self.order_id_end = self.order_id_start+int(self.config['DEFAULT']['order_id_end'])+1

        '''
            1 st Iteration : range(1,21)
            2nd Iteration : range(21,41)
            3rd Iteration : range(41,61)
            orders_state.txt
            40
        '''
        OrdersStateFileUtility.update_last_order_id(self.order_id_end)
        print(OrdersStateFileUtility.read_last_order_id())
        return {
            "order_id": range(self.order_id_start, self.order_id_end),
            "order_date": [self.random_date(self.start_date, self.end_date) for _ in
                           range(self.order_id_end - self.order_id_start)],
            "employee_id": [random.choice(self.employee_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "product_id": [random.choice(self.product_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "customer_id": [random.choice(self.customer_ids) for _ in range(self.order_id_end - self.order_id_start)],
            "quantity_sold": [random.choice(self.quantity_range) for _ in
                              range(self.order_id_end - self.order_id_start)],
        }

    def save_to_csv(self):
        df = pd.DataFrame(self.generate_data())
        df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')
        epoch_time = int(time.time())

        # Generate filenames
        processing_filename = f"{epoch_time}_processing.csv"
        final_filename = f"{epoch_time}.csv"

        # Paths
        processing_csv_path = os.path.join(self.csv_directory, processing_filename)
        final_csv_path = os.path.join(self.csv_directory, final_filename)

        # Save the DataFrame to a file with "_processing"
        with open(processing_csv_path, 'w+', newline='') as file:
            df.to_csv(file, index=False)
        # Short delay to ensure the file is fully written and closed
        time.sleep(1)

        # Rename the file to remove "_processing"
        os.rename(processing_csv_path, final_csv_path)

    def run(self):
        while True:
            self.save_to_csv()
            time.sleep(self.interval_seconds)
