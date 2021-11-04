"""
XETRA ETL PROCESS
"""

from typing import NamedTuple
from xetra.common.connector import S3BucketConnector
from xetra.common.connector import AzureBlobConnector
import logging

from xetra.common.constants import MetaProcessFormat
from xetra.common.meta_process import MetaProcess
import pandas as pd
from datetime import datetime


class XetraSourceConfig(NamedTuple):

    """
    CLASS FOR SOURCE CONFIG DATA

    """
    src_first_extract_date: str
    columns: list
    date: str
    isin: str
    time: str
    start_price: str
    max_price: str
    min_price: str
    traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    CLASS FOR TARGET CONFIG DATA
    """

    key: str 
    key_date_format: str
    format: str
    isin: str
    date: str
    op_price: str
    clos_price: str
    min_price: str
    max_price: str
    daily_traded_vol: str
    prev_clos: str


class XetraETL():
    """
    READ XETRA DATA, APPLY TRANSFORMATIONS, WRITE RESULT DATA TO TARGET SOURCE
    """

    def __init__(self, s3_bucket_src: S3BucketConnector, blob_target: AzureBlobConnector, meta_key: str,
                 src_args: XetraSourceConfig, trgt_args: XetraTargetConfig):

        self._logger = logging.getLogger(__name__)

        self.s3_bucket_src = s3_bucket_src
        self.blob_target = blob_target
        self.meta_key = meta_key
        self.src_args = src_args
        self.trgt_args = trgt_args

        self.extract_date, self.extract_date_list = MetaProcess.get_date_list(first_date = self.src_args.src_first_extract_date,
                                                                              blob_connector = self.blob_target,
                                                                              meta_file_name = self.meta_key)

        self.meta_update_list = self.extract_date_list

    def extract(self):

        self._logger.info('Extracting Xetra Source Files Starting...')
        files = [key for date in self.extract_date_list for key in self.s3_bucket_src.list_files_in_prefix(date)]

        if not files:
            df = pd.DataFrame()

        else:
            df = pd.concat([self.s3_bucket_src.read_csv_to_df(obj)
                            for obj in files], ignore_index=True)
            self._logger.info('Extracting Xetra files complete...')
            return df

    def transform_report1(self, df: pd.DataFrame):
        df = df.loc[:, self.src_args.columns]
        df.dropna(inplace=True)

        # calculating opening price
        df[self.trgt_args.op_price] =   df.sort_values(by=[self.src_args.time]).groupby(
                                                [self.src_args.isin, self.src_args.date])[self.src_args.start_price].transform('first')

        # calculating closing price
        df[self.trgt_args.clos_price] = df.sort_values(by=[self.src_args.time]).groupby(
                                                [self.src_args.isin, self.src_args.date])[self.src_args.start_price].transform('last')

        df.rename(columns={
        self.src_args.min_price: self.trgt_args.min_price,
        self.src_args.max_price: self.trgt_args.max_price,
        self.src_args.traded_vol: self.trgt_args.daily_traded_vol
        }, inplace=True)
        # calculating opening price, closing price eur, daily traded volume, min price, max price.

        df = df.groupby(['ISIN', 'Date'], as_index=False).agg({
            self.trgt_args.op_price : 'min',
            self.trgt_args.clos_price : 'min',
            self.trgt_args.min_price : 'min',
            self.trgt_args.max_price: 'max', 
            self.trgt_args.daily_traded_vol:'sum'
        })


        df[self.trgt_args.prev_clos] = df.sort_values(by=[self.src_args.date]).groupby([self.src_args.isin])[
            self.trgt_args.clos_price].shift(1)

        df[self.trgt_args.prev_clos] = (
            df[self.trgt_args.clos_price] - df[self.trgt_args.prev_clos]) / df[self.trgt_args.prev_clos] * 100

        df = df.round(decimals=2)

        df = df[df.Date >= self.extract_date]

        return df

    def load(self, df:pd.DataFrame):
        # key = 'xetra_daily_report' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'

        key = (
            f'{self.trgt_args.key}'
            f'{datetime.today().strftime(self.trgt_args.key_date_format)}'
            f'{self.trgt_args.format}'

        )
        self.blob_target.write_to_blob(df, key, self.trgt_args.format) 
        MetaProcess.update_meta(self.meta_update_list, self.blob_target, self.meta_key)

        return True

    def etl_report1(self):
        df = self.extract()
        df = self.transform_report1(df)
        self.load(df)
