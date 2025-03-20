package fr.mosef.scala.template.writer.impl

import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}

class PartitionerImpl extends Writer {
  override def write(df: DataFrame, outputPath: String)(implicit spark: SparkSession): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy("categorie_de_produit") // Partitionnement des fichiers de sortie
      .parquet(outputPath)

    println(s"✅ Données écrites en Parquet avec partitionnement par `categorie_de_produit` dans : $outputPath")
  }
}