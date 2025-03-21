package fr.mosef.scala.template.job.impl

import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl

import fr.mosef.scala.template.writer.Writer
import fr.mosef.scala.template.writer.impl.PartitionerImpl
import fr.mosef.scala.template.job.Job
import org.apache.spark.sql.{DataFrame, SparkSession}

class JobImpl(implicit spark: SparkSession) extends Job {

  // Initialisation de Reader avec le SparkSession implicite
  val reader: Reader = new ReaderImpl(spark)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new PartitionerImpl()

  // Spécification des chemins et du format
  val srcPath: String = "src/main/resources/rappelconso0.csv"
  val dstPath: String = "tmp/output"
  val format: String = "csv"  // Ou "parquet"

  override val options: Map[String, String] = Map("header" -> "true", "sep" -> ",")

  // Appel à la méthode `read` avec les bons paramètres
  val inputDF: DataFrame = reader.read(srcPath, format, options)

  // Traitement des données
  val processedDF: DataFrame = processor.process(inputDF)

  // Écriture des données traitées
  writer.write(processedDF, dstPath)
}
