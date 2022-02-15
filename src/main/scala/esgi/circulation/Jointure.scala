package esgi.circulation


object Jointure {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
      .builder()
      .appName("jointure")
      .master("yarn")
      .getOrCreate()


    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)

    print("Test jointure")
    // TODO : lire son fichier d'input et son fichier de jointure
    // val df = ???
    // val joinDf = ???

    // TODO : ajouter ses transformations Spark avec au minimum une jointure et une agrégation

    // TODO : écrire le résultat dans un format pratique pour la dataviz
  }
}