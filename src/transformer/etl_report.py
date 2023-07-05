import logging
from typing import NamedTuple
from datetime import datetime
import pandas as pd
from src.utils import S3Connector
from src.utils.statics import CS



class Config(NamedTuple):
    src_prefix: str
    src_st_year: int
    ida_st_year: str
    src_columns: list
    
class IdaReport:
    def __init__(self, bucket_src: S3Connector, bucket_trg: S3Connector, args: Config):
        self.logger = logging.getLogger("__name__")
        self.bucket_src = bucket_src
        self.bucket_trg = bucket_trg
        self.args = args

    def extract(self):
        """
        Method to extract file objects to a DataFrame
        """
        self.logger.info("Extracting file objects ...")
        objects = self.bucket_src.get_file_objects(self.args.src_st_year,
                    self.args.src_prefix)
        if len(objects) == 0:
            df = pd.DataFrame()
        else:
            df = pd.concat([self.bucket_src.read_csv_object_to_df(obj.key)\
                            for obj in objects], ignore_index=True)
        return df
    
    def transform(self, df):
        """
        Method to transform raw IDA dataset data to compute report
        """
        if df.empty:
            self.logger.info("The dataframe is empty and cannot be transformed!")
            return df
        self.logger.info("Start processing dataframe ...")
        df = df.loc[:, self.args.src_columns]
        df[CS.AGREEMENT_SIGN_DATE] = pd.to_datetime(df[CS.AGREEMENT_SIGN_DATE])
        df[CS.EFFECTIVE_DATE] = pd.to_datetime(df[CS.EFFECTIVE_DATE])
        df[CS.END_OF_PERIOD] = pd.to_datetime(df[CS.END_OF_PERIOD])
        df = df[~pd.isna(df[CS.AGREEMENT_SIGN_DATE])]
        
        df.loc[:, CS.CREDIT_YEAR] = df[CS.AGREEMENT_SIGN_DATE].dt.year
        df[CS.EFFECTIVE_DATE_MONTH] = df[CS.EFFECTIVE_DATE].dt.strftime("%m %Y")
        df = df[df[CS.EFFECTIVE_DATE] >= self.args.ida_st_year]
        
        df_gr = df.groupby([CS.EFFECTIVE_DATE_MONTH, CS.COUNTRY], as_index=False)\
                .agg({ CS.CREDIT_NB: ["count"],
                        CS.ORI_PRINCIPAL_AMOUNT:["sum", "min", "max"],
                        CS.CANCELLED_AMOUNT: ["sum"],
                        CS.CREDITS_HELD: ["sum"],
                        CS.SERVICE_CHARGE_RATE: ["mean"],
                        CS.CREDIT_STATUS: lambda val: val.tolist(),
                        CS.COMMITMENT_CURR: lambda val: val.tolist()})
        df_gr.columns = [" ".join(col).strip() for col in df_gr.columns.to_flat_index()]
        df_gr = df_gr.sort_values(by=[CS.EFFECTIVE_DATE_MONTH])
        return df_gr


    def load_report(self, df):
        """
        Method to load the report dataframe to the target bucket.
        """
        report_date = datetime.today().strftime("%Y%m%d_%H%M%S")
        key = f"ida_statement_report_{report_date}.parquet"
        self.bucket_trg.write_object_to_s3(key, df)
        self.logger.info("Report loaded successfully ...")
    

    def etl_report(self):
        """
        ETL to generate IDA report.
        """
        df = self.extract()
        df = self.transform(df)
        self.load_report(df)
        return True