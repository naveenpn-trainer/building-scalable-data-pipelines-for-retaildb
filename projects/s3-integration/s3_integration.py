import os
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Replace these with your bucket name and local directory path
BUCKET_NAME = 'sunlife-mini-project-bucket'
LOCAL_DIRECTORY = 'C:/LiveDataFolder'

# Initialize S3 client
s3_client = boto3.client('s3')

class UploadToS3(FileSystemEventHandler):
    def on_created(self, event):
        # Check if the created event is for a file and not a directory
        if not event.is_directory and event.src_path.endswith(('.jpg', '.png', '.txt','.csv')):
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            try:
                # Upload the file to S3
                s3_client.upload_file(file_path, BUCKET_NAME, file_name)
                print(f"Uploaded {file_name} to {BUCKET_NAME}")
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")

    def on_modified(self, event):
        """Handle the event when an existing file is modified."""
        if not event.is_directory and event.src_path.endswith(('.csv')):  # Add more file types if needed
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            try:
                s3_client.upload_file(file_path, BUCKET_NAME, file_name)
                print(f"Uploaded {file_name} to {BUCKET_NAME}")
            except Exception as e:
                print(f"Error uploading {file_name}: {e}")

def main():
    event_handler = UploadToS3()
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
