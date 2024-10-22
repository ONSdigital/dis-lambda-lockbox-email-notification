import os
import boto3
import logging
from botocore.exceptions import ClientError

ses_client = boto3.client('ses')

def lambda_handler(event, context):
    file_urls = []

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if "ons-dp-prod-encrypted-datasets" in bucket:
            bucket = bucket.replace("ons-dp-prod-encrypted-datasets", "download.ons.gov.uk/downloads-new")
            email_source = "florence@dp-prod.aws.onsdigital.uk"
        elif "ons-dp-staging-encrypted-datasets" in bucket:
            bucket = bucket.replace("ons-dp-staging-encrypted-datasets", "download.dp-staging.aws.onsdigital.uk/downloads-new")
            email_source = "florence@dp-staging.aws.onsdigital.uk"
        elif "ons-dp-sandbox-encrypted-datasets" in bucket:
            bucket = bucket.replace("ons-dp-sandbox-encrypted-datasets", "download.dp.aws.onsdigital.uk/downloads-new")
            email_source = "florence@dp.aws.onsdigital.uk"
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
            send_email(["publishing@ons.gov.uk"], email_subject, email_body, email_source)
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
