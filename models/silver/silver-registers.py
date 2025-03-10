import sys
import boto3
import pandas as pd
from datetime import datetime

class GlueSilver:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client("s3")
        
        self.table = "registers"
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
            # Remove duplicates based on cpf, email, and id (equivalent to ROW_NUMBER() OVER PARTITION BY)
            df['n_cpf'] = df.groupby('cpf').cumcount() + 1
            df['n_email'] = df.groupby('email').cumcount() + 1
            df['n_id'] = df.groupby('id').cumcount() + 1

            df_filtered = df[(df['n_cpf'] == 1) & (df['n_email'] == 1) & (df['n_id'] == 1)].copy()

            # Concatenate address and cast date fields
            df_filtered['birth_date'] = pd.to_datetime(df_filtered['birth_date']).dt.date
            df_filtered['register_date'] = pd.to_datetime(df_filtered['register_date']).dt.date
            df_filtered['full_address'] = df_filtered['address_street'].astype(str) + " " + df_filtered['address_number'].astype(str)

            # state names to abbreviations and regions
            state_abbr_map = {
                'Acre': 'AC', 'Amapá': 'AP', 'Amazonas': 'AM', 'Pará': 'PA', 
                'Rondônia': 'RO', 'Roraima': 'RR', 'Tocantins': 'TO',
                'Alagoas': 'AL', 'Bahia': 'BA', 'Ceará': 'CE', 'Maranhão': 'MA', 
                'Paraíba': 'PB', 'Pernambuco': 'PE', 'Piauí': 'PI', 
                'Rio Grande do Norte': 'RN', 'Sergipe': 'SE',
                'Distrito Federal': 'DF', 'Goiás': 'GO', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
                'Espírito Santo': 'ES', 'Minas Gerais': 'MG', 'Rio de Janeiro': 'RJ', 'São Paulo': 'SP',
                'Paraná': 'PR', 'Rio Grande do Sul': 'RS', 'Santa Catarina': 'SC'
            }

            region_map = {
                'Norte': ['Acre', 'Amapá', 'Amazonas', 'Pará', 'Rondônia', 'Roraima', 'Tocantins'],
                'Nordeste': ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe'],
                'Centro-Oeste': ['Distrito Federal', 'Goiás', 'Mato Grosso', 'Mato Grosso do Sul'],
                'Sudeste': ['Espírito Santo', 'Minas Gerais', 'Rio de Janeiro', 'São Paulo'],
                'Sul': ['Paraná', 'Rio Grande do Sul', 'Santa Catarina']
            }

            df_filtered['state_abbr'] = df_filtered['state'].map(state_abbr_map).fillna('Unknown')

            df_filtered['region'] = df_filtered['state'].apply(
                lambda x: next((region for region, states in region_map.items() if x in states), 'Unknown')
                )

            # Select final columns
            df_final = df_filtered[[
                'id', 'name', 'birth_date', 'cpf', 'postal_code', 'country', 'city', 'state', 
                'full_address', 'gender', 'marital_status', 'phone', 'email', 
                'register_date', 'state_abbr', 'region'
            ]]

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