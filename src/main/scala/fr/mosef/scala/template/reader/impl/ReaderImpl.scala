package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory

class ReaderImpl extends Reader {
  private val config = ConfigFactory.load()

  // Lecture de la config depuis application.conf
  private val separator = config.getString("file.read_separator")
  private val header = config.getBoolean("file.read_header")
  private val inferSchema = config.getBoolean("file.schema")

  override def read(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", header.toString)
      .option("sep", separator)
      .option("inferSchema", inferSchema.toString)
      .csv(path)
  }
}
