# s3_test_script.py

import boto3

# Initialize the S3 client using environment (aws configure)
s3 = boto3.client('s3')

# Your Bucket Name
BUCKET_NAME = 'finnews-summarizer-project'

# Local test file
local_upload_file = '/tmp/test_upload.txt'
local_download_file = '/tmp/test_download.txt'

# Create a test file
with open(local_upload_file, 'w') as f:
    f.write('Hello from Abhishek\'s team! 🚀')

# Upload File
s3.upload_file(local_upload_file, BUCKET_NAME, 'testing/test_upload.txt')
print(f"✅ Uploaded {local_upload_file} to s3://{BUCKET_NAME}/testing/test_upload.txt")

# Download File
s3.download_file(BUCKET_NAME, 'testing/test_upload.txt', local_download_file)
print(f"✅ Downloaded s3://{BUCKET_NAME}/testing/test_upload.txt to {local_download_file}")
