import shutil, sys
#import pandas
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark = SparkSession.builder.appName("get the number of Airports by Country").getOrCreate()
dfAirport = spark.read.csv('Dataset/airports.csv', header = True, inferSchema = True)
num_cores = int(sys.argv[1])
output_file = sys.argv[2]
dfAirport.repartition(num_cores).rdd.getNumPartitions()
dfAirportsByCountries = dfAirport.groupBy("COUNTRY").count()
dfAirportsByCountries.rdd.coalesce(1).saveAsTextFile("intermediate_folder")
shutil.copy("intermediate_folder/part-00000",output_file)
shutil.rmtree("./intermediate_folder")