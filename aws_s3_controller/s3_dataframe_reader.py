"""
Functions for reading data files from S3 buckets into pandas DataFrames.
"""

import io
import pandas as pd
from .aws_connector import S3_WITHOUT_CREDENTIALS
from .s3_scanner import scan_files_in_bucket_by_regex


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
        index_name = df.columns[0]
        df = df.set_index(index_name)
        
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
