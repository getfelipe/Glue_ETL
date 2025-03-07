import json
import boto3


def lambda_handler(event, context):

    s3_record = event['Records'][0]['s3']
    path_file = s3_record['object']['key']

    folder_table = path_file.split('/')[2]


    # Inicializa o cliente do AWS Glue
    glue = boto3.client('glue')

    try:
        response = glue.start_workflow_run(
            Name=f"workflow-{folder_table}",

        )
        print("Sucesso!!!")
    except Exception as e:
        print(f"Erro ao iniciar o workflow: {e}")

