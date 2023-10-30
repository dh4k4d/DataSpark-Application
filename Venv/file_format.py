import logging, os
from ingest import load_files, display_df, df_count
import get_env_variables as gav

logger = logging.getLogger("FileFormat")

def get_file_format(spark):
    try:
        logger.info("Method get_file_format started...")
        for file in os.listdir(gav.src_olap):
            file_dir = gav.src_olap + '/'+file

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            df_city = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("Displaying dataframe as format - {}...".format(file_format))
            display_df(df_city,'df_city')
            dfCity_count = df_count(df_city)
            print(dfCity_count)
        
        for file in os.listdir(gav.src_oltp):
            file_dir = gav.src_oltp + '/'+file

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            df_city = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("Displaying dataframe as format - {}...".format(file_format))
            display_df(df_city,'df_city')
            df_c = df_count(df_city)
            print(df_c)

    except Exception as exp:
        logger.error("An error occured in get_file_format method.")