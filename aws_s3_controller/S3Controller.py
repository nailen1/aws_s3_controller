import pandas as pd
import os
import re
import io
from shining_pebbles import *
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


### BASIC FUNCTIONS: S3 Bucket Management Functions ###

def scan_files_in_bucket_by_regex(bucket, bucket_prefix, regex, option='key'):
    s3 = boto3.client('s3')
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix[-1] != '/' else bucket_prefix
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
    
            print(f"Files matching the regex '{regex}' in the bucket '{bucket}' with prefix '{bucket_prefix}':")
            for file in files:
                print('-', file)
            return files
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
    Download files from an S3 bucket to a local folder.

    :param bucket: str. The name of the S3 bucket.
    :param regex: str. The regex pattern to match the files.
    :param file_folder_local: str. The local folder to save the files.
    :param bucket_prefix: str. The prefix of the S3 bucket to scan.
    :param file_subfolder_local: str. The local subfolder to save the files (optional).
    """
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix[-1] != '/' else bucket_prefix
    s3 = boto3.client('s3')
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
        print(f'- Download {key} to {local_file_path}')



def upload_files_to_s3(file_folder_local, regex, bucket, bucket_prefix='', file_subfolder_local=None):
    """
    Upload files from a local folder to an S3 bucket that match a given regex pattern.

    :param file_folder_local: str. The local folder containing files to upload.
    :param regex: str. The regex pattern to match the files.
    :param bucket: str. The name of the S3 bucket.
    :param bucket_prefix: str. The prefix of the S3 bucket to upload files.
    :param file_subfolder_local: str. The local subfolder containing files to upload (optional).
    """
    s3 = boto3.client('s3')
    file_folder_local = os.path.join(file_folder_local, file_subfolder_local) if file_subfolder_local else file_folder_local
    file_paths = scan_files_including_regex(file_folder_local, regex, option='path')
    if file_paths:
        print(f'Found {len(file_paths)} files in {file_folder_local} that match the regex pattern.')
    else:
        print(f'No files found in {file_folder_local} that match the regex pattern.')
        return 
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix[-1] != '/' else bucket_prefix
        s3_key = os.path.join(bucket_prefix_with_slash, file_name)
        s3.upload_file(file_path, bucket, s3_key)
        print(f'Uploaded {file_path} to s3://{bucket}/{s3_key}')
    return None

import boto3
import pandas as pd
import io
import re
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def scan_files_in_bucket_by_regex(bucket, bucket_prefix, regex, option='key'):
    """
    Scan files in an S3 bucket that match a given regex pattern.

    :param bucket: str. The name of the S3 bucket.
    :param bucket_prefix: str. The prefix of the S3 bucket to scan.
    :param regex: str. The regex pattern to match the files.
    :param option: str. 'name' to return file names, 'key' to return full paths.
    :return: list. A list of matching files.
    """
    s3 = boto3.client('s3')
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

            print(f"Files matching the regex '{regex}' in the bucket '{bucket}' with prefix '{bucket_prefix}':")
            for file in files:
                print('-', file)
            return files
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

def open_df_in_bucket(bucket, bucket_prefix=None, file_name=None, file_key=None):
    """
    Read a CSV file from a specific folder in an S3 bucket and return it as a pandas DataFrame.

    :param bucket: str. The name of the S3 bucket.
    :param bucket_prefix: str. The prefix of the S3 bucket where the file is located.
    :param file_name: str. The name of the file to read.
    :param file_key: str. The full key of the file to read.
    :return: pandas.DataFrame. The DataFrame containing the file data.
    :raises ValueError: If neither 'file_name' nor 'file_key' is provided.
    """
    if file_name is None and file_key is None:
        raise ValueError("Either 'file_name' or 'file_key' must be provided.")
    
    s3 = boto3.client('s3')
    
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
    Read a CSV file from an S3 bucket that matches a given regex pattern and return it as a pandas DataFrame.

    :param bucket: str. The name of the S3 bucket.
    :param bucket_prefix: str. The prefix of the S3 bucket to scan.
    :param regex: str. The regex pattern to match the files.
    :param index: int. The index of the file to read from the list of matching files.
    :return: pandas.DataFrame. The DataFrame containing the file data.
    """
    bucket_prefix_with_slash = bucket_prefix + '/' if bucket_prefix[-1] != '/' else bucket_prefix
    file_keys = scan_files_in_bucket_by_regex(bucket=bucket, bucket_prefix=bucket_prefix_with_slash, regex=regex, option='key')
    file_key = file_keys[index]
    df = open_df_in_bucket(bucket, file_key=file_key)
    return df

def relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix='', target_prefix='', option='copy'):
    """
    Relocate files between S3 buckets based on a regex pattern.

    :param source_bucket: str. The name of the source S3 bucket.
    :param target_bucket: str. The name of the target S3 bucket.
    :param regex: str. The regex pattern to match the files.
    :param source_prefix: str. The prefix of the source S3 bucket to scan.
    :param target_prefix: str. The prefix of the target S3 bucket.
    :param option: str. 'copy' to copy files, 'move' to move files.
    """
    s3 = boto3.client('s3')
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

    :param source_bucket: str. The name of the source S3 bucket.
    :param target_bucket: str. The name of the target S3 bucket.
    :param regex: str. The regex pattern to match the files.
    :param source_prefix: str. The prefix of the source S3 bucket to scan.
    :param target_prefix: str. The prefix of the target S3 bucket.
    """
    relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix, target_prefix, option='copy')
    return None

def move_files_including_regex_between_s3_buckets(source_bucket, target_bucket, regex, source_prefix='', target_prefix=''):
    """
    Move files between S3 buckets based on a regex pattern.

    :param source_bucket: str. The name of the source S3 bucket.
    :param target_bucket: str. The name of the target S3 bucket.
    :param regex: str. The regex pattern to match the files.
    :param source_prefix: str. The prefix of the source S3 bucket to scan.
    :param target_prefix: str. The prefix of the target S3 bucket.
    """
    relocate_files_between_buckets(source_bucket, target_bucket, regex, source_prefix, target_prefix, option='move')
    return None

def create_subfolder_in_bucket(bucket, bucket_subfolder):
    """
    Create a subfolder in an S3 bucket.

    :param bucket: str. The name of the S3 bucket.
    :param bucket_subfolder: str. The name of the subfolder to create.
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