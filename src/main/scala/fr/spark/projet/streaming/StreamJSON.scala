package fr.spark.projet.streaming
import fr.spark.projet.CONST.ENV_VAR
import fr.spark.projet.utils.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object StreamJson {
  def main(args: Array[String]): Unit = {}

  val schema = (new StructType()
    .add("idDrone",IntegerType)
    .add("zone", (new StructType())
      .add("longitude",FloatType)
      .add("latitude",FloatType)
      .add("RegionState",StringType)
      .add("HouseNumber",StringType)
      .add("StreetName",StringType)
    )
    .add("currentDate",StringType)
    .add("currentTime",StringType)
    .add("violationType",IntegerType)
    .add("infraction",(new StructType())
      .add("code",StringType)
      .add("violationDescription",StringType)
      .add("idPhoto",StringType)
    )
    .add("vehicle",(new StructType())
      .add("plaque",StringType)
      .add("plateType",StringType)
      .add("VehicleBodyType",StringType)
      .add("VehicleMake",StringType)
      .add("VehicleColor",StringType))
    )

  val spark_session = SparkUtils.spark()

  val data_frame = spark_session.readStream.format("kafka").option("kafka.bootstrap.servers", ENV_VAR.localHost.toString).option("subscribe", "json").load().selectExpr("CAST(value AS STRING)")

  val data_streamed = data_frame.select(from_json(col("value"), schema).as("data_streamed")).select(
      col("data_streamed.idDrone"),
      col("data_streamed.zone.*" ),
      col("data_streamed.currentDate").cast(DateType),
      col("data_streamed.currentTime"),
      col("data_streamed.violationType"),
      col("data_streamed.infraction.*"),
      col("data_streamed.vehicle.*")
    )

  import java.util.concurrent.TimeUnit
  // OUTPUT JSON Files each 2 min...
  data_streamed.writeStream
    .option("format", "append").format("csv").option("header", true).option("checkpointLocation", ENV_VAR.__OUTPUT__PATH__LOG__.toString)
    .option("path", ENV_VAR.__OUTPUT__FILES__.toString)
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(2,TimeUnit.MINUTES))
    .start()
    .awaitTermination()

}
