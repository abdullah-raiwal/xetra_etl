"""
FILE TO STORE CONSTANTS
"""

from enum import Enum

class S3FileTypes(Enum):

    CSV = 'csv'
    PARQUET = 'parquet'

class BlobFileTypes(Enum):

    CSV = 'csv'
    PARQUET = 'parquet'

class MetaProcessFormat(Enum):

    """
    FORMATION OF META PROCESS FILE
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'