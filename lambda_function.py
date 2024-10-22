import os
import boto3
import logging
from botocore.exceptions import ClientError

ses_client = boto3.client('ses')

def lambda_handler(event, context):
    file_urls = []

    prod_email = os.environ['PROD_EMAIL']
    staging_email = os.environ['STAGING_EMAIL']
    sandbox_email = os.environ['SANDBOX_EMAIL']

    prod_bucket = os.environ['PROD_BUCKET']
    staging_bucket = os.environ['STAGING_BUCKET']
    sandbox_bucket = os.environ['SANDBOX_BUCKET']

    prod_url = os.environ['PROD_URL']
    staging_url = os.environ['STAGING_URL']
    sandbox_url = os.environ['SANDBOX_URL']

    email_recipient = os.environ['EMAIL_RECIPIENT']

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if "prod" in bucket:
            bucket = bucket.replace(prod_bucket, prod_url)
            email_source = prod_email
        elif "staging" in bucket:
            bucket = bucket.replace(staging_bucket,  staging_url)
            email_source = staging_email
        elif "sandbox" in bucket:
            bucket = bucket.replace(sandbox_bucket, sandbox_url)
            email_source = sandbox_email
        else:
            logging.error(f"unrecognised bucket name: {bucket}")
            continue

        base_url = "https://"
        file_url = f"{base_url}{bucket}/{key}"
        file_urls.append(file_url)


        file_name_with_extension = key.split('/')[-1]
        file_name = file_name_with_extension.rsplit('.', 1)[0]


        email_subject = f"New {file_name} dataset URL for lockbox"
        email_body = f"Download URL for lockbox:\n" + "\n".join(file_urls)

        try:
            send_email([email_recipient], email_subject, email_body, email_source)
        except ClientError as e:
            logging.error(f"failed to send email: {e}")
            return {
                'statusCode': 500,
                'body': f"error sending email: {e}"
            }
        
    return {
        'statusCode': 200,
        'body': 'success'
    }

def send_email(to_addresses, subject, body, source):
    response = ses_client.send_email(
        Destination={
            'ToAddresses': to_addresses
        },
        Message={
            'Body': {
                'Text': {
                    'Data': body
                }
            },
            'Subject': {
                'Data': subject
            }
        },
        Source=source
    )
    return response
