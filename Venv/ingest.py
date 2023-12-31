import logging

logger = logging.getLogger("Ingest")

def load_files(spark, file_format, file_dir):
    try:
        logger.info("load_files() method started...")
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").load(file_dir)

        logger.info("file type - {} dataframe created and reading also completed - {}".format(file_format,df))
        return df
    except Exception as e:
        logger.error("An error occured in load_files() method == {}".format(str(e)))
        raise
    else:
        logger.info("dataframe created successfully which is of {}".format(file_format))

def df_count_elements(df):
    try:
        df_count = df.count()
        logger.info("df_count_elements() method executed - {}".format(df_count))
        return df_count
    except Exception as exp:
        logger.error("An error occured in df_count_elements() method === {}".format(str(exp)))

def mySql_Connection(spark, jdbc_url, table, connection_properties):
    try:
        logger.info("Method mySql_Connection started...")
        sql_df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
        logger.info("MySql 8.0 connection established and find result below ...")
        sql_df.show()
        return sql_df
    except Exception as exp:
        logger.error("An error occured in mySql_Connection() method === {}".format(str(exp)))

def display_df(df_show, df_name):
    df_show = df_show.show()
    return df_show

def printSchema(print_df, dfName):
    try:
        logger.info("printSchema() method started - {}".format(dfName))

        sch = print_df.schema.fields
        for i in sch:
            logger.info(f"\t{i}")
        
    except Exception as exp:
        logger.error("An error occured in printSchema() method === {}".format(exp))

        raise
    else:
        logger.info("printSchema() method execution done.")