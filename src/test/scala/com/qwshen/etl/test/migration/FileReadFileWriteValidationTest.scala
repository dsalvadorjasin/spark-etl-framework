package com.qwshen.etl.test.migration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source

/**
 * Validates the on-prem run of pipeline_fileRead-fileWrite.xml end-to-end:
 * copies the fixtures into /tmp/events, executes the pipeline via Launcher with
 * conf/application-onprem.conf, then asserts schema, inputs, output partitions,
 * data quality and business rules.
 */
class FileReadFileWriteValidationTest extends FunSuite with Matchers with BeforeAndAfterAll {
  protected val resourceRoot: String = getClass.getClassLoader.getResource("").getPath
  // forked scalatest JVM runs with cwd = project basedir
  protected val repoRoot: String = new File(System.getProperty("user.dir")).getCanonicalPath

  protected val expectedSchema: StructType = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("birthyear", IntegerType, nullable = true),
    StructField("timestamp", StringType, nullable = true),
    StructField("process_date", StringType, nullable = true),
    StructField("event_id", LongType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("interested", IntegerType, nullable = true)
  ))

  protected lazy val spark: SparkSession = {
    val s = SparkSession.builder().appName("migration-validation").master("local[*]").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    s
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // stage fixtures exactly like deploy/onprem/run-fileRead-fileWrite.sh
    deleteRecursively(Paths.get("/tmp/events/features"))
    deleteRecursively(Paths.get("/tmp/staging/events"))
    copyDir(Paths.get(s"${repoRoot}/src/test/resources/data"), Paths.get("/tmp/events/data"))
    copyDir(Paths.get(s"${repoRoot}/src/test/resources/scripts"), Paths.get("/tmp/events/scripts"))

    new com.qwshen.etl.Launcher().run(Array(
      "--pipeline-def", s"${resourceRoot}pipelines/pipeline_fileRead-fileWrite.xml",
      "--application-conf", s"${repoRoot}/conf/application-onprem.conf"
    ))
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  private def deleteRecursively(p: java.nio.file.Path): Unit = {
    val f = p.toFile
    if (f.exists()) {
      if (f.isDirectory) f.listFiles().foreach(x => deleteRecursively(x.toPath))
      f.delete()
    }
  }

  private def copyDir(src: java.nio.file.Path, dst: java.nio.file.Path): Unit = {
    Files.createDirectories(dst)
    src.toFile.listFiles().foreach { f =>
      val t = dst.resolve(f.getName)
      if (f.isDirectory) copyDir(f.toPath, t) else Files.copy(f.toPath, t, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def partCsvFiles(dir: File): Seq[File] =
    if (!dir.exists()) Seq.empty
    else dir.listFiles().flatMap(f => if (f.isDirectory) partCsvFiles(f) else Seq(f).filter(_.getName.endsWith(".csv")))

  private def readFeatures(dir: String) =
    spark.read.option("header", "true").option("mode", "FAILFAST").schema(expectedSchema).csv(dir)

  test("schema: CSV headers, partition dirs and FAILFAST read match the expected schema") {
    val root = new File("/tmp/events/features")
    val parts = partCsvFiles(root)
    parts.nonEmpty shouldBe true
    parts.foreach { f =>
      val hdr = Source.fromFile(f).getLines().next()
      hdr shouldBe "user_id,birthyear,timestamp,process_date,event_id"
    }
    root.listFiles().filter(_.isDirectory).foreach { d => d.getName should startWith("gender=") }
    root.listFiles().filter(_.isDirectory).flatMap(_.listFiles().filter(_.isDirectory)).foreach { d =>
      d.getName should startWith("interested=")
    }
    val df = readFeatures("/tmp/events/features")
    df.schema shouldBe expectedSchema
  }

  test("input fixtures: users.csv, train.txt, train.csv structure") {
    val users = spark.read.option("header", "true").csv("/tmp/events/data/users/users.csv")
    users.count() shouldBe 6
    users.columns.toSeq shouldBe Seq("user_id", "birthyear", "gender", "joined-at")

    val rawTxt = Source.fromFile("/tmp/events/data/train/train.txt").getLines().toSeq
    rawTxt.head shouldBe "HDR20240211"
    rawTxt.last shouldBe "TRL0000028"
    rawTxt.last.drop(3).toInt shouldBe 28
    rawTxt.count(l => !l.startsWith("HDR") && !l.startsWith("TRL")) shouldBe 28
    // CRLF endings present in the fixture
    val bin = Files.readAllBytes(Paths.get("/tmp/events/data/train/train.txt"))
    bin.sliding(2).count(w => w(0) == '\r'.toByte && w(1) == '\n'.toByte) should be > 0

    val rawCsv = Source.fromFile("/tmp/events/data/train/train.csv").getLines().toSeq
    rawCsv.head shouldBe "HDR,20240211"
    rawCsv.last shouldBe "TRL|0000028"
    rawCsv.count(l => !l.startsWith("HDR") && !l.startsWith("TRL")) shouldBe 27
    rawCsv.last.split("\\|")(1).toInt shouldBe 28
    // NOTE: the fixture's trailer (28) disagrees with its body (27 data rows); observed, fixture not modified.
  }

  test("output: 28 rows across the expected (gender, interested) partitions") {
    val df = readFeatures("/tmp/events/features")
    df.count() shouldBe 28
    import spark.implicits._
    val counts = df.groupBy($"gender", $"interested").count().collect()
      .map(r => ((Option(r.getAs[String]("gender")), r.getAs[Int]("interested")), r.getAs[Long]("count"))).toMap
    counts shouldBe Map(
      (None, 0) -> 16,
      (None, 1) -> 6,
      (Some("male"), 0) -> 4,
      (Some("male"), 1) -> 2
    )
  }

  test("data quality: nullability, domain, distinctness and process_date format") {
    val df = readFeatures("/tmp/events/features")
    import spark.implicits._
    df.filter($"event_id".isNull || $"timestamp".isNull).count() shouldBe 0
    df.filter(!$"interested".isin(0, 1)).count() shouldBe 0
    df.select($"event_id").distinct().count() shouldBe 27
    val re = "^2021-02-06-System at \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?-28-6$"
    df.filter(!$"process_date".rlike(re)).count() shouldBe 0
  }

  test("business rules: left join over train.txt, only the 9-digit user id matches users.csv") {
    val df = readFeatures("/tmp/events/features").cache()
    df.count() shouldBe 28
    import spark.implicits._
    val matched = df.filter($"user_id".isNotNull)
    matched.count() shouldBe 6
    matched.select($"user_id").distinct().collect().map(_(0)) shouldBe Array("111840914")
    matched.filter($"birthyear" =!= 1997 || $"gender" =!= "male").count() shouldBe 0
    df.filter($"user_id".isNull).filter($"gender".isNotNull || $"birthyear".isNotNull).count() shouldBe 0
    df.filter($"user_id".isNull).count() shouldBe 22
    // NOTE: fixed-length `user:1-9` keeps leading spaces, so only the 9-digit user id
    // matches users.csv (existing framework behaviour, preserved by the migration).
  }
}
