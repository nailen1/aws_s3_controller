import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("aws_s3_controller package initialized")

from .s3_file_control_utils import *

from .s3_scanner import (
    scan_files_in_bucket_by_regex,
    scan_files_including_regex
)

from .s3_transfer import (
    download_files_from_s3,
    upload_files_to_s3,
    relocate_files_between_buckets,
    copy_files_including_regex_between_s3_buckets,
    move_files_including_regex_between_s3_buckets
)

from .s3_dataframe_reader import (
    open_df_in_bucket,
    open_df_in_bucket_by_regex,
    open_excel_in_bucket,
    open_excel_in_bucket_by_regex
)

from .s3_structure import (
    create_subfolder_in_bucket
)

from .s3_special_operations import (
    locate_menu_datasets_from_s3_to_ec2web,
    merge_timeseries_csv_files
)

import sys
import inspect
from . import S3Controller

for name, obj in inspect.getmembers(S3Controller):
    if inspect.isfunction(obj) or inspect.isclass(obj):
        globals()[name] = obj

__all__ = [name for name, obj in inspect.getmembers(S3Controller) if inspect.isfunction(obj) or inspect.isclass(obj)]