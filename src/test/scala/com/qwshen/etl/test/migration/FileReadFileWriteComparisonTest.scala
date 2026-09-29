package com.qwshen.etl.test.migration

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

import java.io.File
import java.io.PrintWriter
import scala.io.Source

/**
 * Compares the on-prem baseline output with the cloud (mock/EMR) output of
 * pipeline_fileRead-fileWrite.xml and writes a markdown report.
 *
 * Activated only when these system properties are all set (pass them via
 * JAVA_TOOL_OPTIONS -D... so they reach the forked scalatest JVM):
 *   -Dcompare.local.dir  -Dcompare.cloud.dir  -Dcompare.report.file
 * Optional:
 *   -Dcompare.execution.file  (text file of "Metric|Value" lines)
 */
class FileReadFileWriteComparisonTest extends FunSuite with Matchers {

  private val props = sys.props
  private val configured = Seq("compare.local.dir", "compare.cloud.dir", "compare.report.file").forall(props.contains)

  test("compare local and cloud outputs") {
    if (!configured) cancel("compare.local.dir, compare.cloud.dir and compare.report.file must all be set")

    val localDir = props("compare.local.dir")
    val cloudDir = props("compare.cloud.dir")
    val reportFile = props("compare.report.file")

    new File(localDir).getCanonicalPath should not be new File(cloudDir).getCanonicalPath

    val spark = SparkSession.builder().appName("migration-comparison").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val expectedSchema = StructType(Seq(
        StructField("user_id", StringType, nullable = true),
        StructField("birthyear", IntegerType, nullable = true),
        StructField("timestamp", StringType, nullable = true),
        StructField("process_date", StringType, nullable = true),
        StructField("event_id", LongType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("interested", IntegerType, nullable = true)
      ))
      val expectedTypes: Seq[(String, DataType)] = expectedSchema.fields.map(f => (f.name, f.dataType))

      def read(dir: String, schema: StructType): DataFrame =
        spark.read.option("header", "true").option("mode", "FAILFAST").schema(schema).csv(dir)

      val local = read(localDir, expectedSchema)
      val cloud = read(cloudDir, expectedSchema)
      val compareCols = expectedSchema.fields.map(_.name).filterNot(_ == "process_date")

      // --- Table 1: per-partition row counts + content diff -----------------
      case class PKey(gender: Option[String], interested: Option[Int])
      def partitionsOf(df: DataFrame): Set[PKey] = df.select("gender", "interested").distinct().collect()
        .map(r => PKey(Option(r.getAs[String]("gender")), Option(r.getAs[Integer]("interested")).map(_.intValue))).toSet
      val allParts = (partitionsOf(local) ++ partitionsOf(cloud)).toSeq.sortBy(p => (p.gender.getOrElse(""), p.interested.getOrElse(-1)))

      val label = (p: PKey) => s"gender=${p.gender.getOrElse("__HIVE_DEFAULT_PARTITION__")}/interested=${p.interested.map(_.toString).getOrElse("__HIVE_DEFAULT_PARTITION__")}"
      def filterPart(df: DataFrame, p: PKey): DataFrame = df.filter(
        (p.gender.map(g => col("gender") === g).getOrElse(col("gender").isNull)) &&
        (p.interested.map(i => col("interested") === i).getOrElse(col("interested").isNull)))

      val sb = new StringBuilder
      sb.append("# fileRead-fileWrite migration comparison report\n\n")
      sb.append(s"- Local: `${localDir}`\n- Cloud: `${cloudDir}`\n\n")
      sb.append("| Partition | Local Rows | Cloud Rows | Status |\n|---|---|---|---|\n")

      var table1Fail = false
      val diffs = new StringBuilder
      var localTotal = 0L; var cloudTotal = 0L
      allParts.foreach { p =>
        val l = filterPart(local, p)
        val c = filterPart(cloud, p)
        val lc = l.count(); val cc = c.count()
        localTotal += lc; cloudTotal += cc
        val lMinusC = l.select(compareCols.map(col): _*).exceptAll(c.select(compareCols.map(col): _*))
        val cMinusL = c.select(compareCols.map(col): _*).exceptAll(l.select(compareCols.map(col): _*))
        val ok = (lc == cc) && lMinusC.isEmpty && cMinusL.isEmpty
        if (!ok) {
          table1Fail = true
          diffs.append(s"\n### Differing rows in ${label(p)}\n")
          diffs.append("Local-only (minus process_date):\n```\n")
          lMinusC.collect().foreach(r => diffs.append(r.mkString(",")).append("\n"))
          diffs.append("```\nCloud-only (minus process_date):\n```\n")
          cMinusL.collect().foreach(r => diffs.append(r.mkString(",")).append("\n"))
          diffs.append("```\n")
        }
        sb.append(s"| ${label(p)} | $lc | $cc | ${if (ok) "MATCH" else "MISMATCH"} |\n")
      }
      sb.append(s"| **TOTAL** | **$localTotal** | **$cloudTotal** | **${if (table1Fail) "FAIL" else "PASS"}** |\n\n")

      // --- Table 2: column types --------------------------------------------
      def partCsvFiles(dir: File): Seq[File] =
        if (!dir.exists()) Seq.empty
        else dir.listFiles().flatMap(f => if (f.isDirectory) partCsvFiles(f) else Seq(f).filter(_.getName.endsWith(".csv")))
      def headersOf(dir: String): Set[String] = partCsvFiles(new File(dir)).flatMap(f => Source.fromFile(f).getLines().next().split(",").map(_.trim)).toSet
      def partitionKeysOf(dir: String): Set[String] = {
        def walk(d: File): Seq[String] = d.listFiles().flatMap { f =>
          if (f.isDirectory) Seq(f.getName.takeWhile(_ != '=')) ++ walk(f) else Seq.empty
        }
        walk(new File(dir)).toSet
      }
      val localHeaders = headersOf(localDir); val cloudHeaders = headersOf(cloudDir)
      val localKeys = partitionKeysOf(localDir); val cloudKeys = partitionKeysOf(cloudDir)

      // second permissive all-string read for cast checks
      val stringSchema = StructType(expectedSchema.fields.map(f => StructField(f.name, StringType, nullable = true)))
      val localStr = read(localDir, stringSchema)
      val cloudStr = read(cloudDir, stringSchema)

      sb.append("| Column | Expected Type | Actual Type | Status |\n|---|---|---|---|\n")
      var table2Fail = false
      expectedTypes.foreach { case (name, dt) =>
        val present = (localHeaders.contains(name) && cloudHeaders.contains(name)) ||
                      (localKeys.contains(name) && cloudKeys.contains(name))
        val actual = if (!present) "missing" else {
          val badLocal = localStr.filter(col(name).isNotNull && col(name).cast(dt).isNull).count()
          val badCloud = cloudStr.filter(col(name).isNotNull && col(name).cast(dt).isNull).count()
          val bad = badLocal + badCloud
          if (bad == 0) dt.simpleString else s"string (${bad} non-castable)"
        }
        val ok = actual == dt.simpleString
        if (!ok) table2Fail = true
        sb.append(s"| $name | ${dt.simpleString} | $actual | ${if (ok) "PASS" else "FAIL"} |\n")
      }
      sb.append("\n")

      // --- Table 3: execution metrics (optional) -----------------------------
      props.get("compare.execution.file").foreach { f =>
        val lines = Source.fromFile(f).getLines().filter(_.nonEmpty).toSeq
        if (lines.nonEmpty) {
          sb.append("| Metric | Value |\n|---|---|\n")
          lines.foreach { l =>
            l.split("\\|", 2) match {
              case Array(k, v) => sb.append(s"| ${k.trim} | ${v.trim} |\n")
              case _ => sb.append(s"| | ${l.trim} |\n")
            }
          }
          sb.append("\n")
        }
      }

      sb.append(diffs.toString)
      val out = new PrintWriter(reportFile)
      try { out.write(sb.toString) } finally { out.close() }

      table1Fail shouldBe false
      table2Fail shouldBe false
    } finally {
      spark.stop()
    }
  }
}
