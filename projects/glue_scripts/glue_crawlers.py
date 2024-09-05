import subprocess
import os

# Define the JSON files
json_files = [
    "./create_crawler_cus.json",
    "./create_crawler_cat.json",
    "./create_crawler_pro.json",
    "./create_crawler_emp.json"
]
def create_crawler(json_file):
    try:
        # Construct the AWS CLI command
        command = ["aws", "glue", "create-crawler", "--cli-input-json", f"file://{json_file}"]

        # Execute the command
        result = subprocess.run(command, capture_output=True, text=True, check=True)

        # Print the output and error (if any)
        print(f"Output for {json_file}:")
        print(result.stdout)
        if result.stderr:
            print(f"Error for {json_file}: {result.stderr}")

    except subprocess.CalledProcessError as e:
        print(f"Failed to create crawler for {json_file}.")
        print(f"Error Code: {e.returncode}")  # Captures the return code
        print(f"Error Output: {e.stderr}")  # Captures the standard error output
