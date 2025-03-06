import sys
import boto3
import pandas as pd
from datetime import datetime

class GlueBronze:
    def __init__(self, bucket_name, path_file, folder_table, name_file):
        self.s3_client = boto3.client("s3")
        
        self.bucket_name = bucket_name
        self.path_file = path_file
        self.folder_table = folder_table
        self.name_file = name_file
        
        self.local_file_path = f"/tmp/{name_file}"  # Temporary file path
        
    def download_from_s3(self):
        """Downloads file from S3 to a local temporary file"""
        try:
            self.s3_client.download_file(self.bucket_name, self.path_file, self.local_file_path)
            return True
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

    def upload_to_s3(self, file_path, s3_path):
        """Uploads a file to S3"""
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            print("Success: File uploaded to S3!")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def main(self):
        if not self.download_from_s3():
            return "Parquet file was not possible to load."

        try:
            df = pd.read_parquet(self.local_file_path)
        except Exception as ex:
            return f"Error reading Parquet: {ex}"
            
        if df.empty:
            return "Empty DataFrame"

        # Define Bronze file name and path
        today_str = datetime.today().strftime("%d%m%Y")
        bronze_file_name = f"bronze_{self.folder_table}_{today_str}.parquet"
        bronze_s3_path = f"bronze/{self.folder_table}/{today_str}/{bronze_file_name}"

        # Save as Parquet
        bronze_local_path = f"/tmp/{bronze_file_name}"
        df.to_parquet(bronze_local_path, index=False)

        # Upload to S3
        self.upload_to_s3(bronze_local_path, bronze_s3_path)

        return "Pipeline executed successfully."

if __name__ == "__main__":
    args = sys.argv[1:]
    params = {}
    for i in range(0, len(args), 2):
        params[args[i]] = args[i + 1]
        
    bucket_name = params.get("--bucket_name")
    path_file = params.get("--path_file")
    folder_table = params.get("--folder_table")
    name_file = params.get("--name_file")

    if None in [bucket_name, path_file, folder_table, name_file]:
        print("Missing required parameters!")
        sys.exit(1)

    bronze_pipeline = GlueBronze(bucket_name, path_file, folder_table, name_file)
    result = bronze_pipeline.main()
    print(result)