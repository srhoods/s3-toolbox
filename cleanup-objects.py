#!/usr/bin/python3.12

import boto3
import signal
import argparse
import sys

access_key=''
secret_key=''

def positive_int(value):
    """ Validate that the input is a positive integer (>0) """
    try:
        ivalue = int(value)
        if ivalue <= 0:
            raise argparse.ArgumentTypeError(f"{value} is not a valid positive integer (>0)")
        return ivalue
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not an integer")

def gather_args():
    parser = argparse.ArgumentParser(description="S3 API Toolbox")
    parser = argparse.ArgumentParser(epilog="Simple program to walk a S3 bucket and permanently remove old object versions.\n\nWill not remove the latest object version by design.", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-b", "--bucket", type=str, help="Specify S3 Bucket", required=True)
    parser.add_argument("-p", "--prefix", type=str, help="Specify Prefix", required=True)
    parser.add_argument("-r", "--retain", type=positive_int, required=True, help="Retained number of versions, starting from latest. (Must be an integer > 0)")
    parser.add_argument("-e", "--endpoint", type=str, help="Custom S3 endpoint (e.g. https://myserver.lab.local)", required=False)
    return parser.parse_args()

def delete_old_versions(bucket_name, prefix, retain_count):
    # Permanently delete old versions of objects while retaining the latest specified number of objects

    paginator = s3.get_paginator('list_object_versions')

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            objects_versions = {}

            # Collect all versions for each object
            if 'Versions' in page:
                for version in page['Versions']:
                    key = version['Key']
                    version_id = version['VersionId']

                    if key not in objects_versions:
                        objects_versions[key] = []

                    objects_versions[key].append(version_id)

            # Delete excess versions
            for key, version_ids in objects_versions.items():
                version_ids.sort(reverse=True)  # Ensure latest versions are first

                if len(version_ids) > retain_count:
                    versions_to_delete = version_ids[retain_count:]

                    for version_id in versions_to_delete:
                        s3.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
                        print(f"Deleted old version of {key} (VersionId: {version_id})")

    except:
        print("An error occured accessing the S3 API/Endpoint - please check your credentials,  bucket and prefix details and try again")
        sys.exit(1)

class GracefulKiller:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        exit()

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

    # Start our delete process running
    delete_old_versions(args.bucket, args.prefix, args.retain)
