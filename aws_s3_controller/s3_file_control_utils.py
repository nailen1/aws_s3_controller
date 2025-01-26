from shining_pebbles import *
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import pandas as pd
import os
import re
import io
from .aws_connector import S3, S3_WITHOUT_CREDENTIALS

### BASIC FUNCTIONS: S3 Bucket Management Functions ###

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
        # Handle None or empty bucket_prefix
        if bucket_prefix:
            bucket_prefix_with_slash = bucket_prefix + '/' if not bucket_prefix.endswith('/') else bucket_prefix
            s3_key = os.path.join(bucket_prefix_with_slash, file_name)
        else:
            s3_key = file_name

        s3.upload_file(file_path, bucket, s3_key)
        print(f'Uploaded {file_path} to s3://{bucket}/{s3_key}')
    return None


def open_df_in_bucket(bucket, bucket_prefix=None, file_name=None, file_key=None):
    """
    Read a CSV file from an S3 bucket into a pandas DataFrame.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_prefix (str, optional): Prefix path within the bucket. Defaults to None.
        file_name (str, optional): Name of the file to read. Required if file_key is not provided.
        file_key (str, optional): Full S3 key of the file. Required if file_name is not provided.

    Returns:
        pandas.DataFrame: DataFrame containing the file contents, or None if an error occurs.

    Raises:
        ValueError: If neither file_name nor file_key is provided.
    """
    if file_name is None and file_key is None:
        raise ValueError("Either 'file_name' or 'file_key' must be provided.")
    
    s3 = S3_WITHOUT_CREDENTIALS
    
    if bucket_prefix is not None and not bucket_prefix.endswith('/'):
        bucket_prefix += '/'
    file_path = f"{bucket_prefix}{file_name}" if file_name is not None else file_key
    
    try:
        content = s3.get_object(Bucket=bucket, Key=file_path)['Body'].read()        
        df = pd.read_csv(io.BytesIO(content))
        
        print(f"Successfully read file: {file_path}")
        print(f"DataFrame shape: {df.shape}")
        
        return df
    
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return None


def open_df_in_bucket_by_regex(bucket, bucket_prefix, regex, index=-1):
    """
    Read a CSV file matching a regex pattern from an S3 bucket into a pandas DataFrame.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_prefix (str): Prefix path within the bucket to limit the search scope.
        regex (str): Regular expression pattern to match against file names.
        index (int, optional): Index of the file to read if multiple files match. Defaults to -1 (last matching file).

    Returns:
        pandas.DataFrame: DataFrame containing the file contents, or None if an error occurs.
    """
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix and bucket_prefix[-1] != '/' else bucket_prefix
    file_keys = scan_files_in_bucket_by_regex(bucket=bucket, bucket_prefix=bucket_prefix_with_slash, regex=regex, option='key')
    file_key = file_keys[index]
    df = open_df_in_bucket(bucket, file_key=file_key)
    return df

def open_excel_in_bucket(bucket, bucket_prefix, file_name):
    """
    Read an Excel file from an S3 bucket into a pandas DataFrame using xlrd engine.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_prefix (str): Prefix path within the bucket where the file is located.
        file_name (str): Name of the Excel file to read.

    Returns:
        pandas.DataFrame: DataFrame containing the Excel file contents.
        dict: Error information if the operation fails, containing 'success' and 'error' keys.
    """
    try:
        s3 = S3_WITHOUT_CREDENTIALS
        response = s3.get_object(
            Bucket=bucket, 
            Key=f"{bucket_prefix}/{file_name}"
        )
        return pd.read_excel(io.BytesIO(response['Body'].read()), engine='xlrd')
    except Exception as e:
        return {"success": False, "error": str(e)}

def open_excel_in_bucket_by_regex(bucket, bucket_prefix, regex):
    """
    Read the latest Excel file matching a regex pattern from an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_prefix (str): Prefix path within the bucket to limit the search scope.
        regex (str): Regular expression pattern to match against file names.

    Returns:
        pandas.DataFrame: DataFrame containing the Excel file contents.
        dict: Error information if the operation fails.

    Note:
        If multiple files match the pattern, reads the last one based on alphabetical ordering.
    """
    file_names = scan_files_in_bucket_by_regex(bucket=bucket, bucket_prefix=bucket_prefix, regex=regex, option='name')
    file_name = file_names[-1]
    return open_excel_in_bucket(bucket=bucket, bucket_prefix=bucket_prefix, file_name=file_name)


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
        print(f"No files to relocate from bucket '{source_bucket}' with prefix '{source_prefix}'")
        return

    for key in files_to_relocate:
        copy_source = {'Bucket': source_bucket, 'Key': key}
        target_key = key.replace(source_prefix, target_prefix, 1) if source_prefix else target_prefix + key

        try:
            # Copy the file to the destination bucket
            s3.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key)
            print(f'Copied file: {key} to {target_key}')
            if option == 'move':
                # Delete the file from the source bucket
                s3.delete_object(Bucket=source_bucket, Key=key)
                print(f'Moved file: {key} to {target_key}')
        except Exception as e:
            print(f'Failed to {option} file {key}: {e}')


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


def create_subfolder_in_bucket(bucket, bucket_subfolder):
    """
    Create a new subfolder (prefix) in an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket.
        bucket_subfolder (str): Name of the subfolder to create.

    Returns:
        None

    Note:
        In S3, folders are virtual and are created by adding a trailing slash to the object key.
    """
    if bucket_subfolder[-1] != '/':
        bucket_subfolder += '/'
    s3 = boto3.resource('s3')
    s3.Object(bucket, bucket_subfolder).put(Body=b'')
    print(f"Subfolder '{bucket_subfolder}' created in bucket '{bucket}'.")
    return None


### SPECIAL USECASE FUNCTIONS: ###


### Maintanace Functions for @rpa ###
def locate_menu_datasets_from_s3_to_ec2web(menu_code, start_date=None, end_date=None, save_date=None):
    bucket_name_protocol = 'dataset-system'
    start_date = start_date or '2020-01-01'
    end_date = end_date or get_date_n_days_ago(get_today("%Y%m%d"), n=1, format="%Y%m%d")
    save_date = save_date or get_today("%Y%m%d")
    regex_menu=f'menu{menu_code}'
    mapping_menu = {
        '2160': f"dataset-timeseries-menu2160-from{start_date.replace('-', '')}-to{end_date.replace('-', '')}-save{save_date.replace('-', '')}",
        '2205': f"dataset-snapshot-menu2205-at{end_date.replace('-', '')}-save{save_date.replace('-', '')}"
    }
    bucket_prefix_protocol = mapping_menu[menu_code]
    download_files_from_s3(bucket=bucket_name_protocol, bucket_prefix=bucket_prefix_protocol, file_folder_local=f'dataset-menu{menu_code}', regex=regex_menu)
    return None


# Timeseries Data Processing Functions #
def merge_timeseries_csv_files(file_path_old, file_path_new, file_name_save=None, file_folder_save=None):
    try:
        old_data = pd.read_csv(file_path_old, dtype=str)
        new_data = pd.read_csv(file_path_new, dtype=str)
    except Exception as e:
        print(f"Error reading files: {e}")
        return

    if old_data.empty:
        raise ValueError(f"The file {file_path_old} is empty.")
    if new_data.empty:
        raise ValueError(f"The file {file_path_new} is empty.")

    date_column = '일자'

    old_data[date_column] = pd.to_datetime(old_data[date_column])
    new_data[date_column] = pd.to_datetime(new_data[date_column])

    old_last_date = old_data[date_column].iloc[-1]
    new_data_to_add = new_data[new_data[date_column] > old_last_date]

    old_data[date_column] = old_data[date_column].dt.strftime('%Y-%m-%d')
    new_data_to_add[date_column] = new_data_to_add[date_column].dt.strftime('%Y-%m-%d')

    combined_data = pd.concat([old_data, new_data_to_add])

    first_date = combined_data[date_column].iloc[1]
    last_date = combined_data[date_column].iloc[-1]

    old_file_name = os.path.basename(file_path_old)
    base_file_name = old_file_name.split('-to')[0]  # 'menu2160-code100060'
    base_menu_code = base_file_name.split('-')[0]
    file_name_save = file_name_save if file_name_save else f"{base_file_name}-to{last_date.replace('-', '')}-save{get_today('%Y%m%d')}.csv"

    if not file_folder_save:
        base_folder_name = f"dataset-timeseries-{base_menu_code}-from{first_date.replace('-', '')}-to{last_date.replace('-', '')}-merge{get_today('%Y%m%d')}"
        file_folder_save = os.path.join('.', base_folder_name)
    
    check_folder_and_create_folder(file_folder_save)
    file_path_save = os.path.join(file_folder_save, file_name_save)
    combined_data.to_csv(file_path_save, index=False)
    print(f"Merged file saved as {file_path_save}")
    
    return combined_data
