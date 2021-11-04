"""
CLASS AND METHODS FOR PROCESSING META FILE
"""
import collections
from datetime import datetime
from typing import Collection
from xetra.common.connector import AzureBlobConnector
import pandas as pd
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFile
from azure.core.exceptions import ResourceNotFoundError
from datetime import timedelta


class MetaProcess():

    @staticmethod
    def update_meta(extract_date_list, connector: AzureBlobConnector, meta_file_name):

        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                       MetaProcessFormat.META_PROCESS_COL.value])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(
            MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)

        try:
            df_old = connector.read_csv_from_blob(meta_file_name)

            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFile

            df_all = pd.concat([df_old, df_new])

        except ResourceNotFoundError:
            df_all = df_new

        connector.write_to_blob(df_all, meta_file_name, 'csv')
        return True

    @staticmethod
    def get_date_list(first_date: str, blob_connector: AzureBlobConnector, meta_file_name: str):

        min_date = datetime.strptime(
            first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)

        today_date = datetime.today().date()

        try:
            df_meta = blob_connector.read_csv_from_blob(meta_file_name)
            dates = [(min_date + timedelta(days=x))
                     for x in range(0, (today_date - min_date).days + 1)]

            src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
            dates_missing = set(dates[1:]) - src_dates

            if dates_missing:
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                return_dates = [date.strftime('%Y-%m-%d')
                                for date in dates if date >= min_date]

                return_min_date = first_date
            else:
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()

        except ResourceNotFoundError:
            return_dates = [(min_date + timedelta(days=x)).strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
                            for x in range(0, (today_date - min_date).days + 1)]
            return_min_date = first_date

        return return_min_date, return_dates
