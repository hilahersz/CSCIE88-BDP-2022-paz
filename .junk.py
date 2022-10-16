import os

import boto3

BUCKET = "hila-hw4"

if __name__ == '__main__':
    key = os.environ["KEY"]
    token = os.environ["TOKEN"]
    region = os.environ["REGION"]

    s3 = boto3.resource(
        service_name='s3',
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=token
    )

    bucket = s3.Bucket(BUCKET).objects.all()



    print("hi")
