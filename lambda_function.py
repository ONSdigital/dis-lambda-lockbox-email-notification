import os
import boto3
import logging
from botocore.exceptions import ClientError

ses_client = boto3.client('ses')

def lambda_handler(event, context):
    file_urls = []

    email_source = os.environ['EMAIL_SOURCE']
    bucket_name = os.environ['BUCKET_NAME']
    download_url = os.environ['DOWNLOAD_URL']

    email_recipient = os.environ['EMAIL_RECIPIENT']

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if bucket_name not in bucket:
            logging.error(f"Unexpected bucket name: {bucket}")
            continue

        bucket = bucket.replace(bucket_name, download_url)

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
