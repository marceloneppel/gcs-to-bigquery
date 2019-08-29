package dev.marceloneppel

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.extra.json._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}

import scala.collection.JavaConverters._

object WordCount {
  case class Record(text: String, number: Integer)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")

    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("text").setType("STRING"),
        new TableFieldSchema().setName("number").setType("INTEGER"),
      ).asJava
    )

    sc.jsonFile[Record](input)
      .applyTransform(ParDo.of(new DoFn[Record, TableRow] {
        @Setup
        private[marceloneppel] def setup(): Unit = ()

        @ProcessElement
        private[marceloneppel] def processElement(c: DoFn[Record, TableRow]#ProcessContext): Unit =
          c.output(new TableRow().set("text", c.element().text).set("number", c.element().number))
      }))
      .saveAsBigQuery(output, schema, Write.WriteDisposition.WRITE_APPEND, Write.CreateDisposition.CREATE_IF_NEEDED, null, TimePartitioning("DAY"))

    sc.close().waitUntilFinish()
  }
}
