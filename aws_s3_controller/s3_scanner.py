"""
Functions for scanning and searching files in S3 buckets and local directories.
"""

import re
import os
from .aws_connector import S3_WITHOUT_CREDENTIALS
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from string_date_controller import extract_date_ref_from_file_name

def scan_files_in_bucket_by_regex(bucket, bucket_prefix, regex, option='key'):
    """
    Scan files in an S3 bucket that match a given regex pattern.

    Args:
        bucket (str): Name of the S3 bucket to scan.
        bucket_prefix (str): Prefix path within the bucket to limit the search scope.
        regex (str): Regular expression pattern to match against file names/paths.
        option (str, optional): Return format option. Either 'key' for full S3 keys or 'name' for file names only. Defaults to 'key'.

    Returns:
        list: List of matching file keys or names, depending on the option parameter.

    Raises:
        NoCredentialsError: If AWS credentials are not found.
        PartialCredentialsError: If AWS credentials are incomplete.
    """
    s3 = S3_WITHOUT_CREDENTIALS 
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix and bucket_prefix[-1] != '/' else bucket_prefix
    pattern = re.compile(regex)
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=bucket_prefix_with_slash)
        files = []
        for page in page_iterator:
            if 'Contents' in page:
                for file in page['Contents']:
                    if pattern.search(file['Key']) and file['Key'] != bucket_prefix_with_slash:
                        files.append(file['Key'])
        if files:
            mapping_option = {
                'name': [file.split('/')[-1] for file in files],
                'key': files
            }
            try:
                files = mapping_option[option]
            except KeyError:
                print(f"Invalid option '{option}'. Available options: {', '.join(mapping_option.keys())}")
                return []
    
            print(f"{len(files)} Files matching the regex '{regex}' in the bucket '{bucket}' with prefix '{bucket_prefix}':")
        else:
            print(f"No files matching the regex '{regex}' found in the bucket '{bucket}' with prefix '{bucket_prefix}'")
            return []
        
    except NoCredentialsError:
        print("Credentials not available.")
        return []
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
    return files


def extract_dates_ref_in_bucket(bucket, bucket_prefix, regex, option_dashed=True):
    """
    Extracts date references from file names in an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_prefix (str): Prefix path within the bucket to limit the search scope.
        regex (str): Regular expression pattern to match against file names.

    Returns:
        list: List of extracted date references in dashed format (YYYY-MM-DD).
    """
    file_names = scan_files_in_bucket_by_regex(bucket=bucket, bucket_prefix=bucket_prefix, regex=regex, option='name')
    dates_ref_existing = [extract_date_ref_from_file_name(file_name, option_dashed=option_dashed) for file_name in file_names]
    return dates_ref_existing

