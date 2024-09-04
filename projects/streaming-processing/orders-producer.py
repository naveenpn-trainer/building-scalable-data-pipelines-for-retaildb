import os
import time
import pandas as pd
from kafka import KafkaProducer
from configparser import ConfigParser


class KafkaCSVProducer:
    def __init__(self, config_file='config.properties'):
        self.config = ConfigParser()
        self.config.read(config_file)

        self.csv_directory = self.config['KAFKA']['csv_directory']
        self.kafka_broker = self.config['KAFKA']['broker']
        self.kafka_topic = self.config['KAFKA']['topic']

        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_broker], api_version=(2, 0, 2),
                                      value_serializer=lambda v: v.encode('utf-8'))

    def produce_messages(self):
        while True:
            for filename in os.listdir(self.csv_directory):

                if filename.endswith(".csv") and not filename.endswith("_processing.csv"):
                    print(filename)
                    csv_path = os.path.join(self.csv_directory, filename)
                    self.send_csv_to_kafka(csv_path)
                    os.remove(csv_path)

            time.sleep(61)

    def send_csv_to_kafka(self, csv_path):
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            message = row.to_json()
            print(message)
            self.producer.send(self.kafka_topic, message)
            print(f"Sent: {message}")

    def close(self):
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer = KafkaCSVProducer()
    try:
        producer.produce_messages()
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.close()
