
import os
import yaml
import logging, logging.config
from src.transformer.etl_report import Config, IdaReport
from src.utils.S3Connector import S3Connector


def main():
    # load config file:
    path = os.path.join(os.getcwd(), "conf", "config.yaml")
    config = yaml.safe_load(open(path))
    # logger config:
    logging.config.dictConfig(config["logging"])
    logger = logging.getLogger(__name__)
    logger.info("Init S3 connectors ...")
    sr_connector = S3Connector(config["s3"]['access_key'], config["s3"]['secret_key'],
                               config["s3"]['endpoint_url'], config["s3"]['src_bucket'])
    tr_connector = S3Connector(config["s3"]['access_key'], config["s3"]['secret_key'],
                               config["s3"]['endpoint_url'], config["s3"]['trg_bucket'])
    parameters = Config(**config["source"])
    logger.info("Start ETL processing ...")
    etl_job = IdaReport(bucket_src=sr_connector, bucket_trg=tr_connector, args=parameters)
    # run the to extract the report
    etl_job.etl_report()
    logger.info("End of Job, the process is executed successfully.")



if __name__ == "__main__":
    main()