package fr.spark.projet.streaming

import fr.spark.projet.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import org.apache.spark.sql.functions._

import scala.io.StdIn
import java.sql.Timestamp

import fr.spark.projet.CONST.ENV_VAR
import org.apache.spark.sql.streaming.Trigger
import org.spark_project.dmg.pmml.False
object StreamingDrone {

  def main(args: Array[String]): Unit = {}

  val spark = SparkUtils.spark()

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "drone")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)","key", "timestamp")

  val streamed_dataframe = df
    .withColumn("timestamp", col("timestamp"))
    .withColumn("value_s", split(col("value"), ";")).select(
    col("timestamp"),
    col("value_s").getItem(0).as("IdDrone").cast(IntegerType),
    col("value_s").getItem(1).as("CurrentDate").cast(DateType),
    col("value_s").getItem(2).as("CurrentTime").cast(StringType),
    col("value_s").getItem(3).as("x").cast(FloatType),

    col("value_s").getItem(4).as("y").cast(FloatType),
    col("value_s").getItem(5).as("RegionState").cast(StringType),
    col("value_s").getItem(6).as("HouseNumber").cast(StringType),
    col("value_s").getItem(7).as("StreetName").cast(StringType),
    col("value_s").getItem(8).as("Violation").cast(IntegerType),

    col("value_s").getItem(9).as("IdPhoto").cast(StringType),
    col("value_s").getItem(10).as("Plaque").cast(StringType),
    col("value_s").getItem(11).as("PlateType").cast(StringType),
    col("value_s").getItem(12).as("VehicleBodyType").cast(StringType),

    col("value_s").getItem(13).as("VehicleMake").cast(StringType),
    col("value_s").getItem(14).as("VehicleColor").cast(StringType),
    col("value_s").getItem(15).as("ViolationCode").cast(IntegerType),
    col("value_s").getItem(16).as("ViolationDescription").cast(StringType)

  )
  import java.util.concurrent.TimeUnit
  streamed_dataframe.writeStream
    .option("format", "append")
    .format("csv")
    .option("header", true)
    .option("checkpointLocation", ENV_VAR.__OTHER_OUTPUT__LOG__.toString)
    .option("path", ENV_VAR.__OTHER_OUTPUT__.toString)
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
    .start()
    .awaitTermination()

}
