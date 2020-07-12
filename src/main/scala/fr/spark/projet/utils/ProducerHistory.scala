package fr.esgi.training.spark.streaming
import org.apache.kafka.clients.producer._
import play.api.libs.json.{Json, OWrites, Writes}
import java.util.{Date, Properties, UUID}

import fr.spark.projet.CONST.ENV_VAR
import org.apache.kafka.common.serialization.StringSerializer

import scala.annotation.tailrec
import scala.util.Try
import scala.util.Random.{alphanumeric, _}

case class Zone(var longitude: Float,
                var latitude: Float,
                var RegionState : String,
                var HouseNumber : String,
                var StreetName : String  )

case class Infraction(var code: String,
                      var violationDescription : String,
                      var idPhoto: String)

case class Vehicle( var plaque : String,
                    var PlateType : String,
                    var VehicleBodyType : String,
                    var VehicleMake : String,
                    var VehicleColor : String)

case class Message( var idDrone : Int ,
                    var zone : Zone,
                    var currentDate : String,
                    var currentTime : String,
                    var violationType: Int,
                    var infraction: Infraction,
                    var vehicle :Vehicle
                  )

class ProducerHistory {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ENV_VAR.localHost.toString)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val TOPIC = "json"

  val producer = new KafkaProducer[String, String](props)

  implicit val localisationJson: Writes[Zone] = Json.writes[Zone]
  implicit val infractionJson: Writes[Infraction] = Json.writes[Infraction]
  implicit val vehicleJson: Writes[Vehicle] = Json.writes[Vehicle]
  implicit val messageJson: Writes[Message] = Json.writes[Message]

  val format = new java.text.SimpleDateFormat("MM/dd/yyyy")

  def tryToInt( s: String ): Option[Int] = Try(s.toInt).toOption

  def ajustHour(x: String,c: String): String = c match {
    case "P" => if (tryToInt(x.substring(0,4)) == None){"1200"}
    else {(x.substring(0,4).toInt+1200).toString}
    case "A" => x.substring(0,4)
    case "N" => "1200"
  }

  def IdRegion(x:String): Int = x match {
    case "NY" => nextInt(7) +1
    case _ => nextInt(2000)+100

  }
  def IdPhoto(): String ={
    val id = List.fill(10) (alphanumeric(1))
    id.mkString

  }

  def TimeViolation(x:String):String={
    x.take(2) +":"+ x.tail(1)+x.tail(2)+":00"
  }
  @tailrec
  final def produce(line: Iterator[String]) {
    if (line.hasNext) {
      val list = line.next().replaceAll(""",(?!(?:[^"]*"[^"]*")*[^"]*$)""","").split(",")

      val desc = if (list.size>39){list.apply(39) } else { " " }
      val dateo =  list.apply(4)
      val hour = list.apply(19)
      val hourstr = if (hour.length > 4) hour.substring(4,5) else "N"

      val drone = nextInt(100)
      val message =        Message(
        idDrone = drone,
        zone = Zone(
          longitude = nextFloat() * 10 + 40,
          latitude = - nextFloat() * 40 - 50,
          RegionState = list.apply(2),
          HouseNumber = list.apply(23),
          StreetName = list.apply(24)
        ),
        currentDate =dateo,
        currentTime = TimeViolation(ajustHour(hour,hourstr)),
        vehicle = Vehicle(
          plaque = list.apply(1),
          PlateType = list.apply(3),
          VehicleBodyType = list.apply(6),
          VehicleMake = list.apply(7),
          VehicleColor = list.apply(33)
        ),
        violationType = 1,
        infraction = Infraction(
          code = list.apply(5),
          violationDescription = desc,
          idPhoto = IdPhoto()
        )

      )
      val jsMsg = Json.toJson(Some(message))

      val record = new ProducerRecord[String, String](TOPIC, jsMsg.toString)
      producer.send(record)
      produce(line)
    }
    else{
      producer.close()
    }
  }
}