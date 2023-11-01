import logging, os
from ingest import load_files, display_df, df_count_elements
import get_env_variables as gav
from data_processing import data_clean

logger = logging.getLogger("FileFormat")

def get_files(spark):
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
            df_medicare = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("Displaying dataframe as format - {}...".format(file_format))
            # display_df(df_medicare,'df_medicare')
            df_medicare_count = df_count_elements(df_medicare)
            logger.info("dataframe city count is - {}".format(df_medicare))
        
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
            # display_df(df_city,'df_city')
            df_city_count = df_count_elements(df_city)
            logger.info("dataframe city count is - {}".format(df_city_count))

            processed_df = data_clean(df_medicare, df_city)
            display_df(processed_df,'processed_df')

    except Exception as exp:
        logger.error("An error occured in get_file_format method.")