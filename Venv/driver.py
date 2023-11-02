import logging, sys, os
from logging.config import fileConfig
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
from file_format import get_files
from ingest import mySql_Connection, df_count_elements, display_df
from data_processing import data_clean


fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger()

def main():
    try:
        spark = get_spark_object(gav.envn, gav.appName)
        logger.warning("Spark object created - {}".format(spark))
        get_current_date(spark)
        #load files
        df_medicare, df_city = get_files(spark)
        # df_count
        df_count_elements(df_medicare)
        df_count_elements(df_city)

        #data_processing
        df_medicare, df_city=data_clean(df_medicare, df_city)

        #display
        # display_df(df_medicare, 'df_medicare')
        display_df(df_city, 'df_city')
        
        # SalesResultDf = mySql_Connection(spark, gav.jdbc_url, gav.table, gav.connection_properties)

    except Exception as e:
        logger.error("Error occured in main module - {}".format(str(e)))

if __name__ == "__main__":
    main()
    logger.info("Application done.")
    sys.exit(1)