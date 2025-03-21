package fr.mosef.scala.template.reader.impl

import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  override def read(srcPath: String, format: String, options: Map[String, String]): DataFrame = {
    format match {
      case "csv" =>
        sparkSession.read
          .option("header", opt²ions.getOrElse("header", "true"))
          .option("sep", options.getOrElse("sep", ","))
          .option("inferSchema", options.getOrElse("inferSchema", "true"))
          .csv(srcPath)
      case "parquet" =>
        sparkSession.read.parquet(srcPath)
      case _ =>
        throw new IllegalArgumentException(s"Format de fichier non supporté : $format")
    }
  }
}
