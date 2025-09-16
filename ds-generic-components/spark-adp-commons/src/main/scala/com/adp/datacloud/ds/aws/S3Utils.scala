package com.adp.datacloud.ds.aws

import com.amazonaws.services.s3.model.{DeleteObjectsRequest, GetObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}

import scala.collection.JavaConverters._
import java.io.File
import java.net.URI

object S3Utils {

  lazy val s3Client = AmazonS3ClientBuilder.defaultClient()

  def downloadS3FileToLocal(s3FilePath: String, localFilePath: String): ObjectMetadata = {
    val s3URI = new AmazonS3URI(s3FilePath)
    downloadS3FileToLocal(s3URI.getBucket, s3URI.getKey, localFilePath)
  }

  def downloadS3FileToLocal(s3BucketName: String,
                            s3Key: String,
                            localFilePath: String): ObjectMetadata = {
    val localFile = new File(localFilePath)
    s3Client.getObject(new GetObjectRequest(s3BucketName, s3Key), localFile)
  }

  def parseS3BucketAndKey(s3Path: String): (String, String) = {
    val uri = new URI(s3Path)
    uri.getHost -> uri.getPath.stripPrefix("/")
  }

  def deleteObjects(s3Path: String): Unit = {
    val (bucket, key) = parseS3BucketAndKey(s3Path)
    val keys = s3Client.listObjects(bucket, key.stripSuffix("/") + "/")
      .getObjectSummaries
      .asScala
      .map(_.getKey)

    if (keys.nonEmpty) {
      s3Client.deleteObjects(
        new DeleteObjectsRequest(bucket)
          .withKeys(keys: _*)
      )
    }
  }
}
