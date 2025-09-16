package com.adp.datacloud.ds.rdbms

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.crypto.FileEncryptionProperties
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport
import org.apache.spark.sql.types.StructType

object dxrParquetFunctions {
  private val logger = Logger.getLogger(getClass)

  def getParquetWriterInstance(
      configMap: Map[String, String],
      filePath: String,
      schema: StructType,
      blockSize: Int           = ParquetWriter.DEFAULT_BLOCK_SIZE,
      pageSize: Int            = ParquetWriter.DEFAULT_PAGE_SIZE,
      compressionCodec: String = "UNCOMPRESSED") = {

    val hadoopConfiguration = new Configuration()

    configMap.foreach {
      case (key, value) =>
        hadoopConfiguration.set(key, value)
    }

//    new com.adp.datacloud.ds.util.ParquetOutputWriter(
//      new Path(filePath),
//      hadoopConfiguration,
//      writeSupport)

    val file: Path = new org.apache.hadoop.fs.Path(filePath)

    val mode: ParquetFileWriter.Mode = ParquetFileWriter.Mode.CREATE

    val writeSupport: WriteSupport[InternalRow] =
      getParquetWriteSupport(hadoopConfiguration, schema)
        .asInstanceOf[WriteSupport[InternalRow]]

    val compressionCodecName: CompressionCodecName =
      CompressionCodecName.fromConf(compressionCodec)

    val dictionaryPageSize: Int = ParquetWriter.DEFAULT_PAGE_SIZE

    val enableDictionary: Boolean = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED

    val validating: Boolean = ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED

    val writerVersion: ParquetProperties.WriterVersion =
      ParquetWriter.DEFAULT_WRITER_VERSION

    val conf: Configuration = hadoopConfiguration

    new ParquetWriter[InternalRow](
      file,
      mode,
      writeSupport,
      compressionCodecName,
      blockSize,
      pageSize,
      dictionaryPageSize,
      enableDictionary,
      validating,
      writerVersion,
      conf)

  }

  def getParquetWriteSupport(conf: Configuration, schema: StructType) = {
    ParquetWriteSupport.setSchema(schema, conf)
    new ParquetWriteSupport
  }

}
