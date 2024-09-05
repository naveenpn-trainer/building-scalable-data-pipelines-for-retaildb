import os
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

BUCKET_NAME = "npntraining"
LOCAL_DIRECTORY = "../../dataset/processed/"

# Initialize S3 client
s3_client = boto3.client('s3')


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            s3_path = f"retaildb/orders/{file_name}"
            try:
                s3_client.upload_file(file_path, BUCKET_NAME, s3_path)
                print(f"Uploaded {file_name} to {BUCKET_NAME}/retaildb/orders/")
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")


def main():
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=LOCAL_DIRECTORY, recursive=True)
    observer.start()
    print(f"Watching directory: {LOCAL_DIRECTORY}")

    try:
        while True:
            pass  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()