package com.adp.datacloud.ds.udfs

import org.apache.spark.sql.SQLContext
import com.adp.datacloud.ds.h2o.Word2VecUtility
import org.apache.spark.sql.SparkSession

object Word2VecEmbeddings {
  
  val word2vecEmbedding = (x: String) => { Word2VecUtility.transformToVector(Word2VecUtility.tokenize(x, false)) }
  val word2vecSimilarity = (x: String, y:String) => { Word2VecUtility.cosineSimilarity(x, y) }
  val word2vecEmbeddingUniq = (x: String) => { Word2VecUtility.transformToVector(Word2VecUtility.tokenize(x, true)) }
  val word2vecSimilarityUniq = (x: String, y:String) => { Word2VecUtility.cosineSimilarity(x, y, true) }
  
  def registerUdfs(implicit sparkSession: SparkSession) = {
    sparkSession.udf.register("word2vec_embedding", word2vecEmbedding)
    sparkSession.udf.register("word2vec_similarity", word2vecSimilarity)
    sparkSession.udf.register("word2vec_embedding_uniq", word2vecEmbeddingUniq)
    sparkSession.udf.register("word2vec_similarity_uniq", word2vecSimilarityUniq)
  }
  
}