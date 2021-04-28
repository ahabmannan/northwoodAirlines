import org.apache.spark.sql.{DataFrame, SparkSession}
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// Creating snowflake connection to test
val user = dbutils.secrets.get("snowflake_keys", "snowflake_username")
val password = dbutils.secrets.get("snowflake_keys", "snowflake_password")

val sfoptions = Map(
  "sfUrl" -> "https://qqa85393.us-east-1.snowflakecomputing.com",
  "sfUser" -> user,
  "sfPassword" -> password,
  "sfDatabase" -> "AIRLINES",
  "sfSchema" -> "PUBLIC",
  "sfWarehouse" -> "COMPUTE_WH"
)

//Creating dataframes from csv's
val flights = spark.read.option("header", "true").format("csv").load("dbfs:/FileStore/shared_uploads/abdulmannanahmed897@gmail.com/")

flights.show(false)

val airline = spark.read.option("header", "true").format("csv").load("/FileStore/tables/airlines.csv")
airline.show(false)
 
//val df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/airports.csv")
val airport = spark.read.option("header", "true").format("csv").load("dbfs:/FileStore/shared_uploads/airports.csv")
airport.show(false)

 val combinedDf = flights.join(airport)
combinedDf.printSchema

//total number of flightsval count = combinedDf.groupBy("AIRLINE", "AIRPORT", "MONTH").count().orderBy("MONTH")

val total_number_of_flights = count.withColumnRenamed("count", "total_number_of_flights")

total_number_of_flights.createOrReplaceTempView("totalNumberofFlights")

total_number_of_flights.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "Total_number_of_flights")
  .mode(SaveMode.Overwrite)
  .save()

%sql

DESC totalNumberofFlights

//Airlines with the largest number of delays
 val delays = combinedDf.groupBy("AIRLINE")
 val selectedDelays = delays.agg(sum($"AIR_SYSTEM_DELAY" + $"SECURITY_DELAY" + $"AIRLINE_DELAY" + $"LATE_AIRCRAFT_DELAY" + $"WEATHER_DELAY").cast("String").as("total")).orderBy($"total".desc)

selectedDelays.createOrReplaceTempView("Delays")

selectedDelays.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "Delays")
  .mode(SaveMode.Overwrite)
  .save()

%sql

DESC Delays
//cancellation reason according to airport"
 val cancellation = combinedDf.select("AIRPORT", "CANCELLATION_REASON")

cancellation.createOrReplaceTempView("Cancellation")

selectedDelays.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "Cancellation")
  .mode(SaveMode.Overwrite)
  .save()

%sql

DESC Cancellation



//delay reasons by airport
// //delay reasons by airport
 val delayReasons = combinedDf.select("AIRPORT", "AIR_SYSTEM_DELAY" ,"SECURITY_DELAY" ,"AIRLINE_DELAY" ,"LATE_AIRCRAFT_DELAY" ,"WEATHER_DELAY", "AIRPORT")

delayReasons.createOrReplaceTempView("delayReasons")

selectedDelays.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "delayReasons")
  .mode(SaveMode.Overwrite)
  .save()

//Percentage of on time
flights.createOrReplaceTempView("ontime_percentage")

var percentage = "SELECT airline,ROUND((COUNT(airline)*100/(SELECT COUNT(*) FROM ontime_percentage)),2) as percentage_ontime FROM ontime_percentage WHERE year = '2015' and  ARRIVAL_DELAY = '0' GROUP BY airline "
spark.sql(percentage)

selectedDelays.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "ontime_percentage")
  .mode(SaveMode.Overwrite)
  .save()



