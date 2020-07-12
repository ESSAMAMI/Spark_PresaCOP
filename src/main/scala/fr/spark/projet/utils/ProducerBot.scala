import org.apache.kafka.common.serialization.StringSerializer
import java.util.UUID
import fr.spark.projet.CONST.ENV_VAR
import fr.spark.projet.utils.SparkUtils
import scala.util.Random._
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}


object ProducerBot extends App {

  import java.util.Properties
  import org.apache.kafka.clients.producer._


  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ENV_VAR.localHost.toString)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "drone"

  val sparkSession = SparkUtils.spark()
  val sc = sparkSession.sparkContext

  import sparkSession.implicits._

  val dataframe = sparkSession.read.format("csv").option("header", "true").option("delimiter", ",").load(ENV_VAR.newFileData.toString)

  def IdPhoto(): String ={
    UUID.randomUUID().toString
  }
  dataframe.sample(10)
  while(true) {

    val infraction = nextFloat()

    val idDrone = nextInt(20)
    val x = nextFloat() * 2 + 42
    val y = -4 * nextFloat() - 72

    val date =  Seq(1).toDF("seq").select(
      current_date().as("current_date"),
      date_format(current_timestamp(),"HH:mm:ss").as("current_time"))

    val currentDate = date.first().getDate(0)
    val currentTime = date.first().getString(1)

    val sample_from_dataframe = dataframe.sample(false, (0.0001)).first()
    println(sample_from_dataframe)
    val RegionState = sample_from_dataframe.getString(2)
    val HouseNumber = sample_from_dataframe.getString(23)
    val StreetName = sample_from_dataframe.getString(24)
    if (infraction > 0.8) {
      val violation = nextFloat()
      val idPhoto = IdPhoto()
      if (violation > 0.99) {
        val record = new ProducerRecord(TOPIC, "key", s"$idDrone;$currentDate;$currentTime;$x;$y;$RegionState" +
          s" $HouseNumber;$StreetName;1;$idPhoto;;;;;;;")
        producer.send(record)
      }
      else {
        val plaque = sample_from_dataframe.getString(1)
        val PlateType = sample_from_dataframe.getString(3)
        val VehicleBodyType = sample_from_dataframe.getString(6)
        val VehicleMake = sample_from_dataframe.getString(7)
        val VehicleColor = sample_from_dataframe.getString(33)
        val violationCode = sample_from_dataframe.getString(5)
        val violationDescription = sample_from_dataframe.getString(39)

        val record = new ProducerRecord(TOPIC, "key", s"$idDrone;$currentDate;$currentTime;$x;$y;$RegionState" +
          s"$HouseNumber;$StreetName;0;$idPhoto;$plaque;$PlateType;$VehicleBodyType;$VehicleMake;$VehicleColor;$violationCode;$violationDescription")
        producer.send(record)
      }
    }
    else {
      val record = new ProducerRecord(TOPIC, "key", s"$idDrone;$currentDate;$currentTime;$x;$y;$RegionState" +
        s" $HouseNumber;$StreetName;;;;;;;;;")
      producer.send(record)
    }
  }
  producer.close()





}