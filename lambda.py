import json
import boto3


def lambda_handler(event, context):
    # Inicializa o cliente do AWS Glue
    glue = boto3.client('glue')

    try:
        response = glue.start_workflow_run(
            Name=f"workflow-registers",

        )
        print("Sucesso!!!")
    except Exception as e:
        print(f"Erro ao iniciar o workflow: {e}")


