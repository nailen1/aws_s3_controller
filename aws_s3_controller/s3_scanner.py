"""
Functions for scanning and searching files in S3 buckets and local directories.
"""

import re
import os
from .aws_connector import S3_WITHOUT_CREDENTIALS
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


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


def scan_files_including_regex(file_folder, regex, option="name"):
    """
    Scan local directory for files matching a regex pattern.

    Args:
        file_folder (str): Path to the directory to scan.
        regex (str): Regular expression pattern to match against file names.
        option (str, optional): Return format option. Either 'name' for file names or 'path' for full file paths. Defaults to 'name'.

    Returns:
        list: Sorted list of matching file names or paths, depending on the option parameter.
    """
    with os.scandir(file_folder) as files:
        lst = [file.name for file in files if re.findall(regex, file.name)]
    mapping = {
        "name": lst,
        "path": [os.path.join(file_folder, file_name) for file_name in lst],
    }
    lst_ordered = sorted(mapping[option])
    return lst_ordered
