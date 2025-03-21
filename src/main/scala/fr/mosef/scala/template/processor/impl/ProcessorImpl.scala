package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ProcessorImpl extends Processor {
  override def process(inputDF: DataFrame): DataFrame = {
    inputDF
      .dropDuplicates() // Suppression des doublons fiche
      .withColumn("processed_at", current_timestamp()) // Ajout d'un timestamp
      .drop("colonne_a_supprimer")
  }
}
