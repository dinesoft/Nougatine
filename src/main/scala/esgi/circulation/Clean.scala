package esgi.circulation


object Clean {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = ???

    val inputFile = args(0)
    val outputFile = args(1)
    // TODO : lire son fichier d'input
    val df = ???

    // TODO : ajouter 3 colonnes à votre dataframe pour l'année, le mois et le jour

    // TODO : écrire le fichier en parquet et partitionné par année / mois / jour
  }
}