import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("aws_s3_controller package initialized")

from .S3Controller import *

import sys
import inspect
from . import S3Controller

for name, obj in inspect.getmembers(S3Controller):
    if inspect.isfunction(obj) or inspect.isclass(obj):
        globals()[name] = obj

__all__ = [name for name, obj in inspect.getmembers(S3Controller) if inspect.isfunction(obj) or inspect.isclass(obj)]