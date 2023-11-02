import logging, os
from ingest import load_files, display_df, df_count_elements
import get_env_variables as gav
from data_processing import data_clean

logger = logging.getLogger("FileFormat")

def get_files(spark):
    try:
        logger.info("get_files() method started...")
        for file in os.listdir(gav.src_olap):
            file_dir = gav.src_olap + '/'+file

            if file.endswith('.parquet'):
                file_format = 'parquet'

            elif file.endswith('.csv'):
                file_format = 'csv'

            df_medicare = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("dataframe created - {}".format(df_medicare))

        
        for file in os.listdir(gav.src_oltp):
            file_dir = gav.src_oltp + '/'+file

            if file.endswith('.parquet'):
                file_format = 'parquet'

            elif file.endswith('.csv'):
                file_format = 'csv'

            df_city = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("dataframe created - {}".format(df_city))

            return(df_medicare, df_city)

    except Exception as exp:
        logger.error("An error occured in get_files() method - {}".format(str(exp)))