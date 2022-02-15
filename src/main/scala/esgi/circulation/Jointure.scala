package esgi.circulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Jointure {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
      .builder()
      .appName("jointure")
      .master("local")
      .getOrCreate()


    val inputFile = args(0) // /data/g3/clean//parquet.....
    val joinFile = args(1)
    val outputFile = args(2)

    print("Test jointure")
    // lire son fichier d'input et son fichier de jointure
    val df_A = spark.read.parquet(inputFile)
    val df_B =  spark.read.parquet(joinFile)

    // ajouter ses transformations Spark avec au minimum une jointure et une agrégation
    val joinDf = df_A.join(df_B, df_A("iu_ac") === df_B("iu_ac"))
    joinDf.groupBy("date_debut").mean("trust").as("Moyenne confiance").show()

    // écrire le résultat dans un format pratique pour la dataviz
    joinDf.write.format("csv").save(outputFile)

  }
}
