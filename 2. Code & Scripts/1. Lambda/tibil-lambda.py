import boto3
import json
import os
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
sf = boto3.client('stepfunctions')
STEP_FUNC_ARN = "arn:aws:states:us-east-1:082105002980:stateMachine:tibil-testfunction"


REQUIRED = {"transaction_id","store_id","product_id","quantity","amount","payment_type","timestamp"}

def lambda_handler(event, context):
    for rec in event.get('Records', []):
        bucket = rec['s3']['bucket']['name']
        key = unquote_plus(rec['s3']['object']['key'])
        obj = s3.get_object(Bucket=bucket, Key=key)
        header_line = obj['Body'].read(4096).decode('utf-8').splitlines()[0]
        headers = {h.strip().lower() for h in header_line.split(',')}
        missing = REQUIRED - headers
        print({"bucket": bucket, "key": key, "missing_headers": list(missing)})
        if missing:
            print(f"Skipping file {key} due to missing headers: {missing}")
            continue
        payload = {
            "s3_bucket": bucket,
            "s3_key": key,
            "run_id": key.split('/')[-1].replace('.csv', '')
        }
        if STEP_FUNC_ARN:
            response = sf.start_execution(
                stateMachineArn=STEP_FUNC_ARN,
                input=json.dumps(payload)
            )
            print(f"Started Step Function execution: {response['executionArn']}")
        else:
            print("STEP_PAYLOAD:", payload)