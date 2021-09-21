# Nyc_yellow_Taxi_Assignement
Perform all the assignment of data transformation process and output insights via a REST endpoint. 

## Dataset used
Data set is used from this page https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Tools used
Azure DataBricks

## Extract
Downloading the CSV files from the website to DBFS of Azure DataBricks

## Transform 
using DBFS, getting the CSV's files, putting it into a Pyspark dataframe ,Cleaning it, casting to correct data types, removing dupilcates and ouputing the file to parquet. In parquet, creating again the pyspark dataframe and  identifying the two insights mentioned and loading it to the database.
   1. Identify in which DOLocationID the tip is highest, in proportion to the cost of the ride, per quarter
   2. Identify at which hour of the day the speed of the taxis is highest (speed = distance/trip duration)

## Load and Consume 
   1. Making the insights available in a AWS RDS SQL Server Database
   2. Serving the insights using API gateway and Lambda in combination

## Architecture
![image](https://user-images.githubusercontent.com/52139405/134149310-fd126154-54f3-40c6-b2f6-31309707adc4.png)



