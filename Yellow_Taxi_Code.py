import os 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib.request

# Creating empty data dictionary 
url_loc = {} 
 
 #getting the data for the year 2020 for yellow taxi csv
for year in range(2020, 2021):
  year_path= '/dbfs/FileStore/taxi_csv/%s' % year
  os.makedirs(year_path,exist_ok= True)
  for m in range(1, 3):
    month = "{:02d}".format(m)
    fname = 'yellow_tripdata_%s-%s.csv' % (year, month)
    url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/%s' % fname
    loc = '/dbfs/FileStore/taxi_csv/%s/%s' % (year,fname)
    url_loc[url] = loc
	
 
 #Downloading the files to DBFS location
for url, loc in url_loc.items():
  urllib.request.urlretrieve(url, loc)
  

 #Reading the CSV's, cleaninng the data by using the correct data types and removing duplicates
unique_path= []
  
for url, loc in url_loc.items():
  path = loc.split('/',2)[2].rsplit('/',1)[0]
  unique_path.append(path)
  
unique_path= set(unique_path)

csvSchema = StructType([StructField("VendorID",IntegerType(),False),
                       StructField("tpep_pickup_datetime",TimestampType(),False),
                       StructField("tpep_dropoff_datetime",TimestampType(),False),
                       StructField("passenger_count",IntegerType(),False),
                       StructField("trip_distance",DoubleType(),False),
                       StructField("RatecodeID",IntegerType(),False),
                       StructField("store_and_fwd_flag",StringType(),False),
                       StructField("PULocationID",IntegerType(),False),
                       StructField("DOLocationID",IntegerType(),False),
                       StructField("payment_type",IntegerType(),False),
                       StructField("fare_amount",FloatType(),False),
                       StructField("extra",FloatType(),False),
                       StructField("mta_tax",FloatType(),False),
                       StructField("tip_amount",FloatType(),False),
                       StructField("tolls_amount",FloatType(),False),
                       StructField("improvement_surcharge",FloatType(),False),
                       StructField("total_amount",FloatType(),False),
                       StructField("congestion_surcharge",FloatType(),False)
                       ]
                      )
                      
for path in unique_path:
  df = spark.read.format("csv").schema(csvSchema).load( path+ "/*.csv")
  
 #removing duplicates
 df.drop_duplicates()
  
 # Writing into to parquet file
 des_name = "Yellow_Taxi_Output"
df.write.mode('overwrite').parquet(path+'/'+des_name)

#Calculation on parquet file
par_df = spark.read.parquet('/'+path+'/'+des_name)
par_df=par_df.withColumn("tip_ratio", round(par_df.tip_amount/par_df.total_amount,2)) \
             .withColumn("trip_duration",(col("tpep_dropoff_datetime").cast(LongType())-col("tpep_pickup_datetime").cast(LongType()))) \
             .withColumn('quarter',quarter(to_date(par_df.tpep_dropoff_datetime))) \
             .withColumn("hour_of_day",hour(par_df.tpep_dropoff_datetime))

par_df=par_df.withColumn("speed",par_df.trip_distance/par_df.trip_duration) 
             

groupby = ["DOLocationID","quarter"]

#Highest tip paid for DOLocationID, in proportion to the cost of the ride, per quarter
df_tip_max_per_DO_per_quarter = par_df.groupby(*groupby).max("tip_ratio").show()

#Highest Speed in the hour of the day 
df_highest_speed_in_hour_of_day = par_df.groupby("hour_of_day").max("speed").orderBy("hour_of_day").show()
  
  
  
  
