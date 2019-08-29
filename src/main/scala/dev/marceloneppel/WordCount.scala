package dev.marceloneppel

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import com.spotify.scio._
import com.spotify.scio.bigquery._

import scala.collection.JavaConverters._

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
    val input = args.getOrElse("input", exampleData)
    val output = args("output")

    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING"),
        new TableFieldSchema().setName("count").setType("INTEGER"),
      ).asJava
    )

    sc.textFile(input)
      .map(_.trim)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .take(2)
      .map(kv => new TableRow().set("word", kv._1).set("count", kv._2))
      .saveAsBigQuery(output, schema, Write.WriteDisposition.WRITE_APPEND, Write.CreateDisposition.CREATE_IF_NEEDED, null, TimePartitioning("DAY"))

    sc.close().waitUntilFinish()
  }
}
