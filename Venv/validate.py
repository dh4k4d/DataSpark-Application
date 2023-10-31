import logging

logger = logging.getLogger("Validate")

def get_current_date(spark):
    try:
        logger.info("Method get_current_date started...")
        output = spark.sql("""SELECT CURRENT_DATE""")
        logger.info("Got current date {}".format(output.collect()))
    except Exception as e:
        logger.error("{} - error occured in get_current_date method.".format(str(e)))
        raise
    else:
        logger.info("Validate done in get_current_date.")