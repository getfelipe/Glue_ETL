import json
import boto3


def lambda_handler(event, context):

    s3_record = event['Records'][0]['s3']
    bucket_name = s3_record['bucket']['name']
    path_file = s3_record['object']['key']

    name_file = path_file.split('/')[-1]
    folder_table = path_file.split('/')[2]

    glue = boto3.client('glue')
    job_name = f'glue-job-registers'
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            '--bucket_name': bucket_name,
            '--path_file': path_file,
            '--folder_table': folder_table,
            '--name_file': name_file
            }
        )
