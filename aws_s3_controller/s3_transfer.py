"""
Functions for transferring files between S3 buckets and local directories.
"""

import os
from .aws_connector import S3_WITHOUT_CREDENTIALS
from .s3_scanner import scan_files_in_bucket_by_regex, scan_files_including_regex


def download_files_from_s3(bucket, regex, file_folder_local, bucket_prefix='', file_subfolder_local=None):
    """
    Download files from an S3 bucket to a local folder based on a regex pattern.

    Args:
        bucket (str): Name of the S3 bucket.
        regex (str): Regular expression pattern to match against file names.
        file_folder_local (str): Local directory path where files will be downloaded.
        bucket_prefix (str, optional): Prefix path within the bucket to limit the search scope. Defaults to ''.
        file_subfolder_local (str, optional): Subdirectory within file_folder_local to save files. Defaults to None.

    Returns:
        None

    Note:
        Creates the local directory structure if it doesn't exist.
    """
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix and bucket_prefix[-1] != '/' else bucket_prefix
    s3 = S3_WITHOUT_CREDENTIALS
    files_keys = scan_files_in_bucket_by_regex(bucket=bucket, bucket_prefix=bucket_prefix_with_slash, regex=regex, option='key')
    print(f'Found {len(files_keys)} files in {bucket} that match the regex pattern.')
    if not os.path.exists(file_folder_local):
        os.makedirs(file_folder_local)

    for key in files_keys:
        print(f'- Downloading {key}...')
        file_name = key.split('/')[-1]
        local_path = os.path.join(file_folder_local, file_subfolder_local) if file_subfolder_local else file_folder_local
        local_file_path = os.path.join(local_path, file_name)

        if not os.path.exists(local_path):
            os.makedirs(local_path)

        s3.download_file(bucket, key, local_file_path)
        print(f'- Save Complete: {local_file_path}')


def upload_files_to_s3(file_folder_local, regex, bucket, bucket_prefix=None, file_subfolder_local=None):
    """
    Upload files from a local directory to an S3 bucket based on a regex pattern.

    Args:
        file_folder_local (str): Local directory containing files to upload.
        regex (str): Regular expression pattern to match against file names.
        bucket (str): Name of the target S3 bucket.
        bucket_prefix (str, optional): Prefix path within the bucket where files will be uploaded. Defaults to None.
        file_subfolder_local (str, optional): Subdirectory within file_folder_local to search for files. Defaults to None.

    Returns:
        None

    Note:
        Files are uploaded maintaining their original names, with optional prefix path in S3.
    """
    s3 = S3_WITHOUT_CREDENTIALS
    file_folder_local = os.path.join(file_folder_local, file_subfolder_local) if file_subfolder_local else file_folder_local
    file_paths = scan_files_including_regex(file_folder_local, regex, option='path')
    
    if file_paths:
        print(f'Found {len(file_paths)} files in {file_folder_local} that match the regex pattern.')
    else:
        print(f'No files found in {file_folder_local} that match the regex pattern.')
        return 
    
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        if bucket_prefix:
            bucket_prefix_with_slash = bucket_prefix + '/' if not bucket_prefix.endswith('/') else bucket_prefix
            s3_key = os.path.join(bucket_prefix_with_slash, file_name)
        else:
            s3_key = file_name

        s3.upload_file(file_path, bucket, s3_key)
        print(f'Uploaded {file_path} to s3://{bucket}/{s3_key}')


def relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix='', target_prefix='', option='copy'):
    """
    Relocate (copy or move) files between S3 buckets based on a regex pattern.

    Args:
        source_bucket (str): Name of the source S3 bucket.
        target_bucket (str): Name of the target S3 bucket.
        regex (str): Regular expression pattern to match against file names.
        source_prefix (str, optional): Prefix path within the source bucket. Defaults to ''.
        target_prefix (str, optional): Prefix path within the target bucket. Defaults to ''.
        option (str, optional): Operation to perform - either 'copy' or 'move'. Defaults to 'copy'.

    Returns:
        None

    Note:
        When option is 'move', files are deleted from the source bucket after successful copy.
    """
    s3 = S3_WITHOUT_CREDENTIALS
    files_to_relocate = scan_files_in_bucket_by_regex(source_bucket, source_prefix, regex, option='key')
    
    if not files_to_relocate:
        print(f"No files found in bucket '{source_bucket}' matching the pattern '{regex}'")
        return None
    
    for source_key in files_to_relocate:
        file_name = source_key.split('/')[-1]
        target_key = os.path.join(target_prefix, file_name) if target_prefix else file_name
        
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key)
        print(f"Copied s3://{source_bucket}/{source_key} to s3://{target_bucket}/{target_key}")
        
        if option == 'move':
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            print(f"Deleted source file: s3://{source_bucket}/{source_key}")
    
    return None


def copy_files_including_regex_between_s3_buckets(source_bucket, target_bucket, regex, source_prefix='', target_prefix=''):
    """
    Copy files between S3 buckets based on a regex pattern.

    Args:
        source_bucket (str): Name of the source S3 bucket.
        target_bucket (str): Name of the target S3 bucket.
        regex (str): Regular expression pattern to match against file names.
        source_prefix (str, optional): Prefix path within the source bucket. Defaults to ''.
        target_prefix (str, optional): Prefix path within the target bucket. Defaults to ''.

    Returns:
        None
    """
    relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix, target_prefix, option='copy')
    return None


def move_files_including_regex_between_s3_buckets(source_bucket, target_bucket, regex, source_prefix='', target_prefix=''):
    """
    Move files between S3 buckets based on a regex pattern.

    Args:
        source_bucket (str): Name of the source S3 bucket.
        target_bucket (str): Name of the target S3 bucket.
        regex (str): Regular expression pattern to match against file names.
        source_prefix (str, optional): Prefix path within the source bucket. Defaults to ''.
        target_prefix (str, optional): Prefix path within the target bucket. Defaults to ''.

    Returns:
        None

    Note:
        Files are deleted from the source bucket after successful copy to the target bucket.
    """
    relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix, target_prefix, option='move')
    return None
