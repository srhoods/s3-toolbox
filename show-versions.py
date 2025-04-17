#!/usr/bin/python3.12

import boto3
import signal
import argparse
import sys
from botocore.exceptions import ClientError
from datetime import datetime
from tabulate import tabulate

access_key=''
secret_key=''

def gather_args():
    parser = argparse.ArgumentParser(description="S3 API Toolbox")
    parser = argparse.ArgumentParser(epilog="Simple program to show object version information for a given key in S3.")
    parser.add_argument("-b", "--bucket", type=str, help="Bucket", required=True)
    parser.add_argument("-k", "--key", type=str, help="Key", required=True)
    parser.add_argument("-e", "--endpoint", type=str, help="Custom S3 endpoint (e.g. https://myserver.lab.local)", required=False)
    return parser.parse_args()

class GracefulKiller:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        print("\n")
        exit()

def list_key_versions(bucket_name, key_name, s3_client):
    try:
        # List object versions in the bucket
        response = s3_client.list_object_versions(Bucket=bucket_name, Prefix=key_name)

        # Filter versions for the specific key
        versions = [
            {
                "Key": version["Key"],
                "Version ID": version["VersionId"],
                "ETag (Hash)": version["ETag"].strip('"'),
                "Size": version.get("Size", "N/A"),
                "Latest": ('Yes' if version["IsLatest"] == True else ''),
                "Last Modified": datetime.strftime(version["LastModified"], '%Y-%m-%d %H:%M:%S')
            }
            for version in response.get('Versions', []) if version["Key"] == key_name
        ]

        if not versions:
            raise ValueError("Key '{}' does not exist in bucket '{}'.".format(key_name, bucket_name))

        # Display the result in tabulated format
        print(tabulate(versions, headers="keys", tablefmt="psql"))

    except:
        print("An error occured accessing the S3 API/Endpoint - please check your credentials,  bucket and prefix details and try again")
        sys.exit(1)

if __name__ == "__main__":
    # Gather our arguments
    args = gather_args()

    # Init our SIGINT/SIGTERM listener
    killer = GracefulKiller()

    # Init S3 client
    if args.endpoint:
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, endpoint_url=args.endpoint)
    else:
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    list_key_versions(args.bucket, args.key, s3)
