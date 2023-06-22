""" Methods used to access AWS Services"""

from io import BytesIO, StringIO
import logging
import re
import pandas as pd
import boto3


class S3Connector:
    """
    Class to connect to AWS S3 service
    """
    def __init__(self, access_key, private_key, endpoint_url, bucket):
        self.logger = logging.getLogger("__name__")
        self.endpoint = endpoint_url
        self.session = boto3.Session(aws_access_key_id = access_key,
                                     aws_secret_access_key = private_key
                                )
        self._s3 = self.session.resource(service_name="s3")
        self._bucket = self._s3.Bucket(bucket)


    def get_file_objects(self, start_year, src_prefix):
        self.logger.info("Loading files objects under the prefix %s "
                         "starting from the year %s ...", src_prefix, start_year)
        objects = self._bucket.objects.filter(Prefix=src_prefix)
        objects = [b for b in objects if 
                      int(re.split("[._]", b.key)[1]) >= start_year]
        return objects
            
    def read_csv_object_to_df(self, key, decode_format="utf-8"):
        self.logger.info("Reading file from S3: %s/%s/%s ..", self.endpoint, self._bucket.name, key)
        buffer = self._bucket.Object(key=key).get().get("Body")\
                                         .read().decode(decode_format)
        buffer_csv = StringIO(buffer)
        df_csv = pd.read_csv(buffer_csv, sep=",")
        return df_csv

    def write_object_to_s3(self, key, df):
        if df.empty:
            self.logger.info("The report dataframe is empty, no data is written!")
            return None
        self.logger.info("Writing file: %s/%s to S3 ...", self._bucket.name, key)
        io_buffer = BytesIO()
        df.to_parquet(io_buffer, index=False)
        self._bucket.put_object(Body=io_buffer.getvalue(), Key=key)

