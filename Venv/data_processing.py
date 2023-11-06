import logging
from pyspark.sql.functions import upper, concat_ws
from ingest import printSchema

logger = logging.getLogger("Data_processing")

def data_clean(cleanDF, fileType):
    try:
        if(fileType=='parquet'):
            # df_city_parquet
            logger.info("data_clean() method started ...")
            cleanDF = cleanDF.drop(cleanDF.city_ascii)
            logger.warning("Column [city_ascii] dropped...")
            cleanDF = cleanDF.select(upper(cleanDF.city).alias("City"), upper(cleanDF.state_name).alias("State_Name"), cleanDF.state_id.alias("State_Id"),upper(cleanDF.county_name).alias("CountryName") , cleanDF.population.alias("Population"),cleanDF.timezone.alias("TimeZone"), cleanDF.zips.alias("ZipCodes"))
            logger.warning("Set-up 'upper case' columns name - City, State_Name, CountryName...")
            printSchema(cleanDF, "cleanDF")

        elif(fileType=='csv'):
        #cleanDF_csv
            cleanDF = cleanDF.withColumn("Name", concat_ws(" ", cleanDF.nppes_provider_first_name, cleanDF.nppes_provider_last_org_name))
            cleanDF= cleanDF.select(cleanDF.npi.alias("Id"), cleanDF.Name, cleanDF.nppes_provider_city.alias("City"),
            cleanDF.nppes_provider_state.alias("State"), cleanDF.drug_name.alias("Drug_name"), cleanDF.total_claim_count.alias("Claim_Count"),
            cleanDF.total_day_supply.alias("Total_day_supply"), cleanDF.total_drug_cost.alias("Total_drug_cost"), cleanDF.years_of_exp.alias("Exp_years"))
            cleanDF = cleanDF.na.drop()
            logger.warning("Deleted NA value rows - [cleanDF]")
            printSchema(cleanDF, "cleanDF")

        return cleanDF
        
    except Exception as exp:
        logger.error("An error occured in data_clean() menthod === ",str(exp))

