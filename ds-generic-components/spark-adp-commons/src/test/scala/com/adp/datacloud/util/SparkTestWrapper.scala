package com.adp.datacloud.util

import com.amazonaws.auth.{AWSSessionCredentials, DefaultAWSCredentialsProviderChain}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

case class CatalogTableMetaData(
    name: String,
    database: String,
    location: String,
    compressed: Boolean,
    serde: Option[String],
    provider: Option[String]           = None,
    tracksPartitionsInCatalog: Boolean = false,
    partitionColumnNames: Seq[String]  = Seq.empty,
    tableProperties: String)

trait SparkTestWrapper extends AnyFunSuite with BaseTestWrapper with BeforeAndAfterAll {

  // log events to view from local history server
  val eventLogDir = s"/tmp/spark-events/"
  scala.reflect.io
    .Path(eventLogDir)
    .createDirectory(force = true, failIfExists = false)

  lazy val wareHouseLocation = s"$projectRootPath/build/spark-warehouse/$appName"
  scala.reflect.io
    .Path(wareHouseLocation)
    .createDirectory(force = true, failIfExists = false)

  lazy val metastoreDbRoot = s"$projectRootPath/build/metastore_db"
  scala.reflect.io
    .Path(metastoreDbRoot)
    .createDirectory(force = true, failIfExists = false)

  lazy val dxrJdbcURL = "jdbc:oracle:thin:@" + dxrDNSAWSParam

  val useLocalES = false

  lazy implicit val sparkSession: SparkSession =
    getTestSparkSessionBuilder().getOrCreate()

  def appName: String

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def addFileToSparkByName(fileName: String): String = {
    val filePath = new java.io.File(fileName).getCanonicalPath
    addFileToSparkByPath(filePath)
    filePath
  }

  def addFileToSparkByPath(filePath: String): Unit = {
    sparkSession.sparkContext.addFile(filePath)
  }

  def displayAllTables(dbname: String)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val metaDataSeq = sparkSession.sessionState.catalog
      .listTables(dbname)
      .map(identifier => {
        val metadata = sparkSession.sessionState.catalog
          .getTableMetadata(identifier)
        //        println(metadata)
        CatalogTableMetaData(
          metadata.identifier.table,
          metadata.database,
          metadata.location.toString,
          metadata.storage.compressed,
          metadata.storage.serde,
          metadata.provider,
          metadata.tracksPartitionsInCatalog,
          metadata.partitionColumnNames,
          metadata.storage.properties
            .map(p => p._1 + "=" + p._2)
            .mkString("[", ", ", "]"))
      })

    val metaDataDS               = sparkSession.sparkContext.parallelize(metaDataSeq, 2).toDS()
    val tablesDS: Dataset[Table] = sparkSession.catalog.listTables(dbname)
    tablesDS.join(metaDataDS.drop('database), Seq("name")).show(500, truncate = false)
  }

  def cleanAndSetupDatabase(dbname: String = "test"): Unit = {
    setUpDatabase(dbname, clean = true)
  }

  def setUpDatabase(dbname: String = "test", clean: Boolean = false): String = {

    val dbPath    = s"$wareHouseLocation/$dbname.db"
    val directory = new Directory(new File(dbPath))

    if (clean) {
      sparkSession.sql(s"""DROP DATABASE IF EXISTS $dbname CASCADE""")
      if (directory.exists)
        directory.deleteRecursively()
    }
    sparkSession.sql(
      s"""CREATE DATABASE IF NOT EXISTS $dbname COMMENT "$dbname" LOCATION "$dbPath" """)

    dbPath
  }

  def printSparkConfAsJson(): Unit = {
    println(writePretty(sparkSession.conf.getAll))
  }

  def createTableFromResource(
      dbname: String,
      tableName: String,
      resourceName: String,
      format: String         = "parquet",
      resourcePrefix: String = File.separator) = {
    val dataPath = getPathByResourceName(resourceName, resourcePrefix)
    sparkSession.sql(s"DROP TABLE IF EXISTS ${dbname}.${tableName}")

    val datadf = format match {
      case ("csv" | "CSV") => {
        sparkSession.read
          .options(Map("header" -> "true", "inferSchema" -> "true"))
          .csv(dataPath)
      }
      case _ => sparkSession.read.parquet(dataPath)
    }

    datadf.printSchema()
    datadf.write.saveAsTable(s"${dbname}.${tableName}")
  }

  def getLocalESSparkConfig(): SparkConf = {
    (new SparkConf)
      .set("spark.es.nodes", "localhost")
      .set("spark.es.port", "9200")
      .set("spark.es.protocol", "http")
  }

  /**
   * there params are set during spark submit via orchestration
   */
  def getESSparkConfig(): SparkConf = {
    (new SparkConf)
      .set("spark.es.nodes", esDomainAWSParam)
      .set("spark.es.port", esPortAWSParam)
      .set("spark.es.protocol", esProtocolAWSParam)
      .set("spark.es.net.http.auth.user", esUserName)
  }

  def getTestSparkSessionBuilder(): SparkSession.Builder = {

    val metaStoreDir = s"$metastoreDbRoot${File.separator}$appName"
    val directory    = new Directory(new File(metaStoreDir))
    if (directory.exists)
      directory.deleteRecursively()

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
      .asInstanceOf[AWSSessionCredentials]
    val builder: SparkSession.Builder = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.eventLog.enabled", value = true)
      .config("spark.eventLog.dir", eventLogDir)
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metaStoreDir;create=true")
      .config("spark.sql.warehouse.dir", wareHouseLocation)
      .config("spark.ui.enabled", true)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.es.nodes.wan.only", true)
      //      .config("spark.files.overwrite", true)
      .config("spark.es.net.ssl", false)
      .config("spark.es.nodes.client.only", false)
      .config("spark.es.index.auto.create", true)
      .config(
        "spark.orchestration.stateMachine.execution.id",
        "com.adp.datacloud.util.sparkTestWrapper")
      .config("spark.orchestration.cluster.id", "sparkTestWrapper")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        classOf[DefaultAWSCredentialsProviderChain].getCanonicalName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      /*.config("spark.hadoop.fs.s3.awsAccessKeyId", credentials.getAWSAccessKeyId)
      .config("spark.hadoop.fs.s3.awsSecretAccessKey", credentials.getAWSSecretKey)
      .config("spark.hadoop.fs.s3.awsSecretAccessKey", credentials.getAWSSecretKey)
      .config("spark.hadoop.fs.s3a.access.key", credentials.getAWSAccessKeyId)
      .config("spark.hadoop.fs.s3a.secret.key", credentials.getAWSSecretKey)
      .config("spark.hadoop.fs.s3a.session.token", credentials.getSessionToken)*/
      .enableHiveSupport()

    val builderWithES = if (useLocalES) {
      builder.config(getLocalESSparkConfig)
    } else {
      builder.config(getESSparkConfig)
    }

    val builderWithESAndS3Proxy = if (System.getProperty("http.proxyHost", "").nonEmpty) {
      builder
        .config("spark.hadoop.fs.s3a.proxy.host", System.getProperty("http.proxyHost"))
        .config("spark.hadoop.fs.s3a.proxy.port", System.getProperty("http.proxyPort"))
    } else {
      builderWithES
    }

    builderWithESAndS3Proxy
  }

  protected override def afterAll(): Unit = {
    cleanUpSparkSession()
  }

  private def cleanUpSparkSession(): Unit = {
    sparkSession.close()
    val metaStoreDir = s"$metastoreDbRoot/$appName"
    FileUtils.deleteDirectory(new File(wareHouseLocation))
    FileUtils.deleteDirectory(new File(metaStoreDir))
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

}
