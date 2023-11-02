import logging

logger = logging.getLogger("Validate")

def get_current_date(spark):
    try:
        logger.info("get_current_date() method started...")
        output = spark.sql("""SELECT CURRENT_DATE""")
        logger.info("Got result - {}".format(output.collect()))
    except Exception as e:
        logger.error("An error occured in get_current_date() method - {}".format(str(e)))
        raise
    else:
        logger.info("get_current_date() method execution done in Validate file.")