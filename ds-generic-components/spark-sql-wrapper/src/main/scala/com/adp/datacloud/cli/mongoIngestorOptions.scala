package com.adp.datacloud.cli

import com.adp.datacloud.ds.aws.SecretsStore
import org.apache.spark.SparkFiles

import scala.io.Source


case class MongoIngestorConfig(
                                file: String = "",
                                inputParams: List[String] = List(),
                                dbName: String = "",
                                collectionName: String = "",
                                username: String = "",
                                pwd: Option[String] = None,
                                domainName: String = "",
                                port: String = "",
                                files: Option[String]                = None
                              ) {
  def sql = {
    val sourcequery = (if (new java.io.File(file).exists()) {
      // file resides in driver/executor working directory in YARN Cluster mode and hence can be accessed directly
      Source.fromFile(file)("UTF-8")
    } else {
      Source.fromFile(SparkFiles.get(file))("UTF-8")
    }).getLines().map {
      _.replaceAll("^\\s*\\-\\-.*$", "").replaceAll("^\\s*$", "")
    }.filter(!_.isEmpty()).mkString("\n")

    //Replacing the hiveconf variables
    inputParams.foldLeft(sourcequery) { (y, x) =>
      ("\\$\\{hiveconf:" + x.split("=")(0) + "\\}").r.replaceAllIn(y, x.split("=")(1))
    }.replaceAll("\\$\\{hiveconf:[a-zA-Z0-9]+\\}", "")
  }

  def getMongoDBURI(): String = {
    s"mongodb://$username:$getPassword@$domainName:$port/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
  }

  lazy val secretMap: Map[String, Any] = SecretsStore.getSecretMapByName("DS_DocDB_Credentials").get

  def getPassword(): String = {
    getSecretValue(username, pwd)
  }

  def getSecretValue(keyName: String, argValue: Option[String]): String = {
    try {
      val credential = argValue match {
        case Some(y) => y
        case None => secretMap(keyName).asInstanceOf[String]
      }
      credential
    } catch {
      case ex: Exception => {
        System.err.println("Invalid Credentials Configuration")
        throw ex
      }
    }

  }


}

object mongoIngestorOptions {

  def parse(args: Array[String]) = {
    val parser = new scopt.OptionParser[MongoIngestorConfig]("Mongo Data Ingestor") {

      head("Mongo DB Data Ingestor", this.getClass.getPackage().getImplementationVersion())
      help("help").text("Prints usage")

      override def showUsageOnError = true

      // required params
      opt[String]('f', "ingest-sql").required().action((arg, config) =>
        config.copy(file = arg)).text("Input Sql string")

      opt[String]('d', "mongo-db-name").required().action((arg, config) =>
        config.copy(dbName = arg)).text("Mongo DB target database name")

      opt[String]('c', "mongo-collection-name").required().action((arg, config) =>
        config.copy(collectionName = arg)).text("Mongo DB target collection Name")

      opt[String]('n', "mongo-domain-name").action((arg, config) =>
        config.copy(domainName = arg)).text("Mongo DB hostName")

      opt[String]('p', "mongo-port").action((arg, config) =>
        config.copy(port = arg)).text("Mongo DB portName")

      opt[String]("mongo-username").required().action((arg, config) =>
        config.copy(username = arg)).text("Mongodb username")
      // optional params

      opt[String]('c', "hiveconf").unbounded().action((arg, config) =>
          config.copy(inputParams = config.inputParams ++ List(arg))).text("Hiveconf variables to replaced in query")

      opt[String]("mongo-password").action((arg, config) =>
        config.copy(pwd = Some(arg))).text("Mongodb password. Not secure and hence not recommended. By default, password shall be lookedup from Secretstore")

      opt[String]("files")
        .action((x, c) => c.copy(files = Some(x)))
        .text("csv string dependent files with absolute paths")

    }

    parser.parse(args, MongoIngestorConfig()) match {
      case Some(config) => config
      case None => {
        System.exit(1)
        null // Error message would have been displayed
      }
    }

  }

}