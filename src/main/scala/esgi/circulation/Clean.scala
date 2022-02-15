package esgi.circulation
import org.apache.spark.sql.SparkSession


object Clean {
  def main(args: Array[String]): Unit = {
    // créer son SparkSession
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", "5")
      .appName("Nougatine")
      .master("local[*]")
      .getOrCreate()


    val inputFile = args(0)
    val outputFile = args(1)
    //  lire son fichier d'input


    println("##spark read text files from a directory into RDD")

    val df = spark.read.csv(inputFile)

    df.na.drop(Seq("Etat trafic")).show(false)

    df.write.parquet(outputFile)
  }
}
