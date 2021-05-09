import shutil, sys
#import pandas
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("find airports whose latitude is between [10, 90] and longitude is between [-10, -90]").getOrCreate()
dfAirport = spark.read.csv('Dataset/airports.csv', header = True, inferSchema = True)
num_cores = int(sys.argv[1])
output_file = sys.argv[2]
dfAirport.repartition(num_cores).rdd.getNumPartitions()
dfAirport = dfAirport.filter((dfAirport["LATITUDE"] >= 10) & (dfAirport["LATITUDE"] <= 90) & (dfAirport["LONGITUDE"] >= -90) & (dfAirport["LONGITUDE"] <= -10))
#dfAirport.select("NAME").write.save("intermediate_folder")
dfAirportNames = dfAirport.select(dfAirport.NAME)
dfAirportNames.rdd.coalesce(1).saveAsTextFile("intermediate_folder")
shutil.copy("intermediate_folder/part-00000",output_file)
shutil.rmtree("./intermediate_folder")