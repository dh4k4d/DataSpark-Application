import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
import logging, sys, os
from logging.config import fileConfig
from ingest import load_files, display_df

fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger()

def main():
    try:
        logger.info('get_spark_object method calling...')
        spark = get_spark_object(gav.envn, gav.appName)
        logger.info("Spark object created - {}".format(spark))
        #get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            file_dir = gav.src_olap + '/'+file
            # print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            df_city = load_files(spark=spark, file_format=file_format, file_dir=file_dir, header=header, inferSchema=inferSchema)
            logger.info("Displaying dataframe data...")
            display_df(df_city,'df_city')

        logger.info("Reading file which is of > {}".format(file_format))

    except Exception as e:
        logger.error("Error occured in main module - {}".format(str(e)))

if __name__ == "__main__":
    main()
    logger.info("Application done.")
    sys.exit(1)