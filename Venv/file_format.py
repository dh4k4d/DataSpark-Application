import logging, os
from ingest import load_files, display_df, df_count_elements
import get_env_variables as gav
from data_processing import data_clean

logger = logging.getLogger("FileFormat")

def get_files(spark, source_dir):
    try:
        logger.info("get_files() method started...")
        for file in os.listdir(source_dir):
            file_dir = source_dir + '/'+file

            logger.warning("I'm from getfiles() count :{} method - {}".format(os.listdir(source_dir),file_dir))

            if file.endswith('.parquet'):
                file_format = 'parquet'

            elif file.endswith('.csv'):
                file_format = 'csv'

            logger.info("Creating dataframe ..")
            dataframe = load_files(spark=spark, file_format=file_format, file_dir=file_dir)
            logger.info("dataframe created - {} : {}".format(source_dir, dataframe))
            return(dataframe)

    except Exception as exp:
        logger.error("An error occured in get_files() method - {}".format(str(exp)))