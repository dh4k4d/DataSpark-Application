import logging
from pyspark.sql.functions import lower, col, upper

logger = logging.getLogger("Data_processing")

def data_clean(df_medicare,df_city):
    try:
        logger.info("data_clean() method started ...")
        logger.warning("Column city_ascii droping..")
        df_city = df_city.drop(df_city.city_ascii)
        logger.warning("Making upper case to City, State_Name, CountryName...")
        df_city = df_city.select(upper(df_city.city).alias("City"), upper(df_city.state_name).alias("State_Name"), df_city.state_id.alias("State_Id"),upper(df_city.county_name).alias("CountryName") , df_city.population.alias("Population"),df_city.timezone.alias("TimeZone"), df_city.zips.alias("ZipCodes"))

        logger.info("Printing Schema of df_city processed dataframe.")
        printSchema(df_city)
        logger.info("Returing processed dataframe.")
        return df_city
        
    except Exception as exp:
        logger.error("An error occured in data_clean() menthod === ",str(exp))

def printSchema(print_df):
    print_df = print_df.printSchema()
    return print_df
