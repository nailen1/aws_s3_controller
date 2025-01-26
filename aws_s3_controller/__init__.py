import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("aws_s3_controller package initialized")

from .aws_connector import S3, S3_WITHOUT_CREDENTIALS
from .aws_consts import *
from .s3_scanner import *
from .s3_transfer import *
from .s3_structure import *
from .s3_dataframe_reader import *
from .s3_special_operations import *
from .alias import *

__all__ = [
    'S3',
    'S3_WITHOUT_CREDENTIALS',
    'scan_files_in_bucket_by_regex',
    'download_files_from_s3',
    'scan_files_including_regex',
    'upload_files_to_s3',
    'open_df_in_bucket',
    'open_df_in_bucket_by_regex',
    'open_excel_in_bucket',
    'open_excel_in_bucket_by_regex',
    'relocate_files_between_buckets',
    'copy_files_including_regex_between_s3_buckets',
    'move_files_including_regex_between_s3_buckets',
    'create_subfolder_in_bucket',
    'locate_menu_datasets_from_s3_to_ec2web',
    'merge_timeseries_csv_files'
]