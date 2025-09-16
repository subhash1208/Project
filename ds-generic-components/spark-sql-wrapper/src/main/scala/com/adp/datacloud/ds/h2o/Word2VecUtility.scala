package com.adp.datacloud.ds.h2o

import hex.genmodel.MojoModel
import hex.genmodel.MojoReaderBackendFactory
import hex.genmodel.MojoReaderBackendFactory.CachingStrategy
import hex.genmodel.algos.word2vec.Word2VecMojoModel
import org.apache.log4j.Logger

object Word2VecUtility {

  private val logger = Logger.getLogger(getClass)

  val stopWords = Set("ourselves", "hers", "between", "yourself", "but", "again", "there", "about","miscellaneous",
    "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do",
    "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as",
    "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her",
    "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up",
    "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can",
    "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing",
    "it", "how", "further", "was", "here", "than",
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday","am","pm","shift",
		"misc", "rd", "nd", "join", "available","multiple"
    ).map(_.toLowerCase()).toArray

  val word2VecmojoUrl = getClass.getClassLoader.getResource("h2o-jobsnskills-word2vec.zip")

  val word2vecModel = try {
    MojoModel.load(MojoReaderBackendFactory.createReaderBackend(word2VecmojoUrl, CachingStrategy.MEMORY)).asInstanceOf[Word2VecMojoModel]
  } catch {
    case e: Exception => {
      logger.error(e.getMessage, e)
      null.asInstanceOf[Word2VecMojoModel]
    }
  }

  val transformToVector = (inputSeq: Seq[String]) => {
    val zeroArray = Array.fill(word2vecModel.getVecSize) { 0.toFloat }

    // calculate average of all tokens and return a single vector

    val aggregateVectorTuple = inputSeq.foldLeft((zeroArray, 0))((baseArrayTuple, nextToken) => {
      val word2VecArray = word2vecModel.transform0(nextToken, new Array[scala.Float](word2vecModel.getVecSize))

      // skip word count for averaging if a word vector is null
      if (word2VecArray != null) {
        (baseArrayTuple._1.zip(word2VecArray).map(x => x._1 + x._2), baseArrayTuple._2 + 1)
      } else {
        (baseArrayTuple._1, baseArrayTuple._2)
      }
    })

    if (aggregateVectorTuple._2 > 0) {
      Some(aggregateVectorTuple._1.map(_.toDouble / aggregateVectorTuple._2))
    } else {
      None
    }
  }

  val tokenize = (inputString: String, removeDuplicates: Boolean) => {
    val tokens = inputString.toLowerCase().split("\\s").filter({ x => !x.isEmpty && x.length>=2 && !stopWords.contains(x) })
    if (removeDuplicates) tokens.toSet.toArray else tokens
  }

  def cleanAndTokenize (inputString: String, removeDuplicates: Boolean = false): Array[String] = {
    // remove all numbers and non word characters
    tokenize(inputString.replaceAll("\\d+", " ").replaceAll("\\W+", " "), removeDuplicates)
  }

  def cosineSimilarity(a: String, b: String, removeDuplicates: Boolean = false): Double = {
    if (a != null && b != null) {
      val x = transformToVector(cleanAndTokenize(a, removeDuplicates)).getOrElse(Array[Double]())
      val y = transformToVector(cleanAndTokenize(b, removeDuplicates)).getOrElse(Array[Double]())
      cosineSimilarity(x, y)
    } else {
      null.asInstanceOf[Double]
    }
  }

  def cosineSimilarity(a: Array[Double], b: Array[Double]): Double = {
    if (a.size > 0 && b.size > 0) {
      require(a.size == b.size)
      dotProduct(a, b) / (magnitude(a) * magnitude(b))
    } else {
      null.asInstanceOf[Double]
    }
  }

  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct = ((a: Array[Double], b: Array[Double]) => {
    a.zip(b).foldLeft(0.0)((finalSum, i) => { (i._1 * i._2) + finalSum })
  })

  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude = ((input: Array[Double]) => {
    math.sqrt(input.foldLeft(0.0)((magnitudeValue, i) => {
      (i * i) + magnitudeValue
    }))
  })

  def main(args: Array[String]) {}

}