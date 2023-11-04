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
        #get_current_date(spark)
        #load files
        olapDF = get_files(spark, gav.src_oltp)
        fileType = 'parquet'

        # df_count
        df_count_elements(olapDF)

        #data_processing
        cleanDF = data_clean(olapDF, fileType)

        #df_count
        df_count_elements(cleanDF)

        #display
        display_df(cleanDF, 'dfResult')
        
        # SalesResultDf = mySql_Connection(spark, gav.jdbc_url, gav.table, gav.connection_properties)

    except Exception as e:
        logger.error("Error occured in main module - {}".format(str(e)))

if __name__ == "__main__":
    main()
    logger.info("Data Processing App Closed.")
    sys.exit(1)