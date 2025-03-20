package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.impl.PartitionerImpl

trait Job {
  val reader: Reader
  val processor: Processor
  val writer: Writer
  val src_path: String
  val dst_path: String

  def run()(implicit spark: SparkSession): Unit = {
    try {
      println(s"📥 Lecture des données depuis : $src_path")
      val inputDF: DataFrame = reader.readFile(src_path)

      println("🔄 Transformation des données en cours...")
      val processedDF: DataFrame = processor.process(inputDF)

      println(s"💾 Écriture des données transformées vers : $dst_path")
      writer.write(processedDF, dst_path)

      println("✅ Job terminé avec succès !")
    } catch {
      case e: Exception =>
        println(s"❌ Erreur dans le job : ${e.getMessage}")
        throw e
    }
  }
}


# à mettre dans un fichier Myjob
object MyJob extends Job {
  override val reader = new CsvReader()
  override val processor = new Transformer()
  override val writer = new Writer()
  override val src_path = "data/input.csv"
  override val dst_path = "data/output"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("MyJob")
      .master("local[*]") // Pour exécuter localement
      .getOrCreate()

    run()

    spark.stop()
  }
}