import sys
import boto3
import pandas as pd
from datetime import datetime

class GlueSilver:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client("s3")
        
        self.table = "orders"
        self.bucket_name = bucket_name
        #self.folder_table = folder_table
        self.today_str = datetime.today().strftime("%d%m%Y")

        self.path_file = f"bronze/{self.table}/{self.today_str}/bronze_file.parquet"
        
         # Temporary file path
        self.tmp_silver_file = f"/tmp/bronze_file.parquet"



    def download_from_bronze_s3(self):
        """Downloads file from bronze S3 to a local temporary file"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"bronze/{self.table}/{self.today_str}")
            contents = response.get("Contents")

            if not contents:
                print("No files found in the specified S3 location.")
                return False


            #path_bronze_file = contents[0].get("Key")
            #bronze_file = path_bronze_file.split("/")[-1]

            
            self.s3_client.download_file(self.bucket_name, self.path_file, self.tmp_silver_file)
 
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

    def data_transformation(self, df):
        try:
            df["order_value"] = df["order_value"].astype(float)
            df["charges"] = df["charges"].astype(float)
            df["discount_value"] = df["discount_value"].astype(float)
            df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

            # Criando a coluna n_id equivalente a ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id)
            df["n_id"] = df.groupby("order_id").cumcount()

            # Filtrando apenas onde n_id == 0
            filtered_raw_orders = df[df["n_id"] == 0].copy()

            # Criando as colunas calculadas
            filtered_raw_orders["total_value"] = (
                filtered_raw_orders["order_value"] + 
                filtered_raw_orders["charges"] - 
                filtered_raw_orders["discount_value"]
            )

            filtered_raw_orders["used_voucher"] = filtered_raw_orders["voucher"].notna().astype(int)

            # Selecionando apenas as colunas finais desejadas
            df_final = filtered_raw_orders[
                ["order_id", "cpf", "order_value", "charges", "discount_value", "total_value", 
                "used_voucher", "order_status", "order_date"]
            ]

            return df_final
        
        except Exception as ex:
            print("Erro na data_transformation: ", ex)
            return pd.DataFrame()


    def main(self):
        if not self.download_from_bronze_s3():
            return "Parquet file was not possible to load."

        try:
            df = pd.read_parquet(self.tmp_silver_file)
        except Exception as ex:
            return f"Error reading Parquet: {ex}"
            
        if df.empty:
            return "Empty DataFrame"


        df_final = self.data_transformation(df)

        # Define Bronze file name and path
        
        #silver_file_name = f"silver_{self.folder_table}_{self.today_str}.parquet"
        silver_s3_path = f"silver/{self.table}/{self.today_str}/silver_file.parquet"

        # Save as Parquet
        silver_local_path = f"/tmp/silver_file.parquet"
        df_final.to_parquet(silver_local_path, index=False)

        # Upload to S3
        self.upload_to_s3(silver_local_path, silver_s3_path)

        return "Silver executed successfully."

if __name__ == "__main__":
    args = sys.argv[1:]
    params = {}
    for i in range(0, len(args), 2):
        params[args[i]] = args[i + 1]
        
    bucket_name = params.get("--bucket_name")
    #folder_table = params.get("--folder_table")

    if None in [bucket_name]:
        print("Missing required parameters!")
        sys.exit(1)

    glue_pipeline = GlueSilver(bucket_name)
    result = glue_pipeline.main()
    print(result)
