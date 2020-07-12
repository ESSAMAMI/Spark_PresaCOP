package fr.spark.projet.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  val sparkConf = new SparkConf()

  def initExercice(exercice : String) = {
    sparkConf.set("spark.app.name", exercice)
  }

  def spark() = {
    sparkConf.set("spark.master", "local[4]")

    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    session
  }


}