""" CLASSES AND METHODS TO CREATE CONNECTION TO AZURE BLOB CONTAINER AND AWS S3 BUCKET(public)"""

import os
import boto3
from botocore.client import Config
from botocore import UNSIGNED
from azure.storage.blob import BlobServiceClient
import logging
import pandas as pd
from io import StringIO, BytesIO
from xetra.common.custom_exceptions import WrongFormatException


########################################### S3 CONNECTOR CLASS ##############################################


class S3BucketConnector():
    """
    class and methods to connect s3 bucket
    """

    def __init__(self, service_name: str, bucket_name: str):
        """
        param: service_name = name of aws service to connect (s3)
        param: bucket_name  = name of public aws bucket to connect
        """

        self._logger = logging.getLogger(__name__)
        self._s3 = boto3.resource(service_name=service_name,
                                  config=Config(signature_version=UNSIGNED))

        self._bucket = self._s3.Bucket(bucket_name)

    def read_csv_to_df(self, key: str, sep: str = ','):
        """
        reads csv file from xetra bucket and returns as pandas DataFrame
        """

        self._logger.info("Reading file %s/%s", self._bucket.name, key)

        csv_obj = self._bucket.Object(key=key).get()[
            'Body'].read().decode('utf-8')
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def list_files_in_prefix(self, prefix: str):
        """
        list all file names from S3 bucket with given prefix 
        """
        files = [
            obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

########################################### AZURE BLOB CLASS ##############################################


class AzureBlobConnector():

    """
    class and methods to connect to azure blob storage
    """

    def __init__(self, connection_string: str, container_name: str):
        """
        param: connection_string = connection string to connect to azure cloud.
        param: container_name    = name of container to connect and store processed data.
        """
        self._logger = logging.getLogger(__name__)

        self._blob_client = BlobServiceClient.from_connection_string(
            connection_string)
        self._container = self._blob_client.get_container_client(
            container_name)

    def read_csv_from_blob(self, meta_file_name: str):
        meta_file = self._container.get_blob_client(meta_file_name)

        with open('meta.csv', "wb") as my_blob:
            blob_data = meta_file.download_blob()
            blob_data.readinto(my_blob)

        df = pd.read_csv('meta.csv')
        return df

    def write_to_blob(self, dataframe: pd.DataFrame, filename, fileformat):
        """
        this method helps to upload csv and parquet to azure blob container

        params: dataframe -> pandas dataframe to be uploaded
        params: filename  -> in case of parquet file its unique key that generates every time
                             in case of csv, it can be same(like metafile) or different

        paramL fileformat -> format destination file format (like .csv or .parquet)
        """

        if dataframe.empty:
            return None

        elif fileformat == 'csv':
            self._logger.info('Writing csv to blob storage...')
            blob_client = self._container.get_blob_client(blob=filename)
            blob_client.upload_blob(
                dataframe.to_csv(index=False), overwrite=True)

            return True

        elif fileformat == 'parquet':

            parquet_file = BytesIO()
            dataframe.to_parquet(parquet_file, engine='pyarrow')
            parquet_file.seek(0)
            self._logger.info('Writing Transformed Dataframe to Blob...')
            blob_client = self._container.get_blob_client(blob=filename)
            blob_client.upload_blob(parquet_file)
            self._logger.info('DataFrame has been uploaded sucessfully...')

            return True

        self._logger.info(
            'the file format %s is not supported to written to blob container', fileformat)
        raise WrongFormatException
