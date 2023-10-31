import logging

logger = logging.getLogger("Ingest")

def load_files(spark, file_format, file_dir):
    try:
        logger.info("Files loading started...")
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).load(file_dir)
        logger.info("file type - {} dataframe created - {}".format(file_format,df))
        return df
    except Exception as e:
        logger.error("An error occured at load files == {}".format(str(e)))
        raise
    else:
        logger.info("dataframe created successfully which is of {}".format(file_format))

def display_df(df_show, df_name):
    df_show = df_show.show()
    return df_show

def df_count(df_city):
    df_count = df_city.count()
    logger.info("Method df_count executed - {}".format(df_count))
    return df_count

def mySql_Connection(spark, jdbc_url, table, connection_properties):
    logger.info("Method mySql_Connection started...")
    sql_df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
    logger.info("MySql 8.0 connection established and find result below ...")
    sql_df.show()
    return sql_df