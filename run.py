"""
RUNNING THE XETRA ETL JOB
"""
import logging
import logging.config
from numpy.lib.utils import source
import yaml
from xetra.common.connector import S3BucketConnector, AzureBlobConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig,XetraTargetConfig  
import argparse

def main():
    """
    ENTRY POINT TO RUN XETRA ETL JOB
    """
    parser = argparse.ArgumentParser(description='Run the xetra etl job')
    parser.add_argument('config', help='a config file in YML format')
    args = parser.parse_args()
    # confg_path = 'M:/Projects/AWS_S3_ETL/xetra_etl/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(args.config))

    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("this is a test.")

    # reading s3 and azure blob configs
    s3_config = config['s3']
    blob_config = config['blob']

    #creating innstances of s3 and blob
    s3_bucket_src = S3BucketConnector(service_name=s3_config['service_name'], bucket_name=s3_config['src_bucket_name'])
    blob_container_target = AzureBlobConnector(blob_config['conn_str'], blob_config['container'])

    # reading source & target configs

    source_configs = XetraSourceConfig(**config['source'])
    target_configs = XetraTargetConfig(**config['target'])

    # reading meta config
    meta_config = config['meta']
    
    # create XetraETL instance and start etl job
    logging.info('ETL Job started...')
    xetra_etl = XetraETL(s3_bucket_src=s3_bucket_src,
                         blob_target=blob_container_target,
                         meta_key=meta_config['meta_key'], src_args=source_configs, trgt_args=target_configs)

    xetra_etl.etl_report1()
    logger.info('Finished ETL job..')


if __name__ == "__main__":
    main()
