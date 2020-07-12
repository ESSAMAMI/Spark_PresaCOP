package fr.spark.projet.utils
import fr.esgi.training.spark.streaming.ProducerHistory
import fr.spark.projet.CONST.ENV_VAR
import scala.annotation.tailrec
import scala.io.Source

object ProducerJSON {
  def main(args: Array[String]): Unit = {

    val get_file_from_list = List(
      ENV_VAR.originalFileInput.toString
    )

    def start_streaming(x: String): Any = {
      val input = Source.fromFile(x).getLines()
      input.next()
      val producer = new ProducerHistory
      producer.produce(input)
    }
    @tailrec
    def matchCsv(x: List[String]): Any = x match {
      case head::tail => start_streaming(head);matchCsv(tail)
      case head::Nil => start_streaming(head)
      case _ => println("An error was occured !")
    }
    matchCsv(get_file_from_list)
  }
}