
import os
import yaml
import logging, logging.config
from dotenv import load_dotenv
from src.transformer.etl_report import Config, IdaReport
from src.utils.S3Connector import S3Connector
from src.utils.statics import CS


def load_config():
    load_dotenv("env.list")
    path = os.path.join(os.getcwd(), "conf", "config.yaml")
    with open(path) as file:
        yaml_content = file.read()
    substituted_yaml = os.path.expandvars(yaml_content)
    config = yaml.safe_load(substituted_yaml)
    # cast date -> str
    config["source"][CS.IDA_YEAR] = str(config["source"][CS.IDA_YEAR])
    return config

def main():
    # load config file:
    config = load_config()
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