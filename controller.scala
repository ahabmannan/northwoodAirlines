import org.apache.spark.sql.{DataFrame, SparkSession}
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// Use secrets DBUtil to get Snowflake credentials.
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
val df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/abdulmannanahmed897@gmail.com/partition_01.csv")
val airline = spark.read.option("header", "true").format("csv").load("/FileStore/tables/airlines.csv")
airline.show()

// Function to write table to snowflake

airline.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "AIRTRAFFIC")
  .mode(SaveMode.Overwrite)
  .save()

val select = airline.select("AIRLINE")

select.write
  .format("snowflake")
  .options(sfoptions)
  .option("dbtable", "NO_OF_AIRPORTS")
  .mode(SaveMode.Overwrite)
  .save()

val df: DataFrame = spark.read
  .format("snowflake") // or just use "snowflake"
  .options(options)
  .option("dbtable", "AIRPORT")
  .load()

df.show(false)

val select = df.select("IATA_CODE", "AIRPORT")
select.show(false)

airlinesdf.write
  .format("snowflake")
  .options("dbtable", "AIRLINES")
  .mode(SaveMode.Overwrite)
  .save()

airportsdf.write
  .format("snowflake")
  .options("dbtable", "AIRPORTS")
  .mode(SaveMode.Overwrite)
  .save()

val airport = spark.read.option("header", "true").format("csv").load("/FileStore/tables/airlines.csv")

println(airport.count)

val renameCol = airline.withColumnRenamed("AIRLINE", "AIRLINE1")
val aa = airport.join(renameCol)
val flights = spark.read.option("header", "true").format("csv").load("dbfs:/FileStore/shared_uploads/")
val combinedDf = flights.join(aa)

//total number of flights
val count = combinedDf.groupBy("AIRLINE1", "AIRPORT", "MONTH").count().orderBy("MONTH")

//rename count 
//Airlines with the largest number of delays
val delays = combinedDf.groupBy("AIRLINE")
val selectedDelays = delays.agg(sum($"AIR_SYSTEM_DELAY" + $"SECURITY_DELAY" + $"AIRLINE_DELAY" + $"LATE_AIRCRAFT_DELAY" + $"WEATHER_DELAY").cast("String").as("total")).orderBy($"total".desc)

//cancellation reason according to airport"
val cancellation = combinedDf.select("AIRPORT", "CANCELLATION_REASON")

//delay reasons by airport
val delayReasons = combinedDf.select("AIRPORT", "AIR_SYSTEM_DELAY" ,"SECURITY_DELAY" ,"AIRLINE_DELAY" ,"LATE_AIRCRAFT_DELAY" ,"WEATHER_DELAY", "AIRPORT").show(100000, false)



