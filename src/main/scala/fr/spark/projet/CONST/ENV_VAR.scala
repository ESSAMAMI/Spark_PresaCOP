package fr.spark.projet.CONST

object ENV_VAR extends Enumeration
{

  val  localHost = Value("127.0.0.1:9092")
  val inputData = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/new_data_drone.csv")
  val originalFileInput = Value("C:\\Users\\hamza\\RestutitionDeDonnées\\Projet\\drone_data.csv")
  val newFileData = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/new_data_drone.csv")

  // OUTPUT
  /*JSON*/
  val __OUTPUT__PATH__LOG__ = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/JSON/log/")
  val __OUTPUT__FILES__ = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/JSON/output/")

  /*OTHER*/
  val __OTHER_OUTPUT__LOG__ = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/log/")
  val __OTHER_OUTPUT__ = Value("D:/Cours/4_IABD/SPARK/PrestaCop/src/data/output/")





}