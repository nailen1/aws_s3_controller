"""
Functions for managing S3 bucket structure and organization.
"""

from .aws_connector import S3_WITHOUT_CREDENTIALS


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
    
    s3 = S3_WITHOUT_CREDENTIALS
    s3.put_object(Bucket=bucket, Key=bucket_subfolder)
