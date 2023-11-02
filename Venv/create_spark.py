from pyspark.sql import SparkSession
import logging

logger = logging.getLogger("Create_spark")

def get_spark_object(envn, appName):
    try:
        logger.info("get_spark_object method() started...")
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'
        logger.info("master set-up as {}".format(master))
        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        return spark

    except Exception as exp:
        return exp
    
