import logging, sys, os
from logging.config import fileConfig
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
from file_format import get_files
from ingest import mySql_Connection, df_count_elements

fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger()

def main():
    try:
        logger.info('get_spark_object method calling...')
        spark = get_spark_object(gav.envn, gav.appName)
        logger.info("Spark object created - {}".format(spark))
        # get_current_date(spark)
        get_files(spark)
        # SalesResultDf = mySql_Connection(spark, gav.jdbc_url, gav.table, gav.connection_properties)
        # df_count(SalesResultDf)

    except Exception as e:
        logger.error("Error occured in main module - {}".format(str(e)))

if __name__ == "__main__":
    main()
    logger.info("Application done.")
    sys.exit(1)