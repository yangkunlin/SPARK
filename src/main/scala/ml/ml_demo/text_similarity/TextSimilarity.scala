package ml.ml_demo.text_similarity

import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.sql.SparkSession

/**
  * @author YKL on 2018/4/27.
  * @version 1.0
  *          spark： 
  *          梦想开始的地方
  */
object TextSimilarity {

  def testSimilarity(sparkSession: SparkSession) = {

    val path = "E:\\DATANALYSIS\\src\\main\\resources\\20news-bydate-test\\*"
    val rdd = sparkSession.sparkContext.wholeTextFiles(path)

    //file 是 文件 dir，text是文件内容
    val text = rdd.map { case (file, text) => text }

    //split text on any non-word tokens
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))

    //filter out numbers
    val regex =
      """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)

    //examine potential stopwords
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)

    // filter out stopwords
    val stopwords = Set(
      "the", "a", "an", "of", "or", "in", "for", "by", "on", "but", "is", "not", "with", "as", "was", "if",
      "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
    )
    //    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
    //
    //    //filter out tokens less than 2 characters
    //    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
    //
    //    //filter out rare tokens with total occurence < 2
    val rareTokens = tokenCounts.filter { case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    //    val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }

    //create a function to tokenize each document
    def tokenize(line: String): Seq[String] = {
      line.split("""\W+""")
        .map(_.toLowerCase)
        .filter(token => regex.pattern.matcher(token).matches)
        .filterNot(token => stopwords.contains(token))
        .filterNot(token => rareTokens.contains(token))
        .filter(token => token.size >= 2)
        .toSeq
    }

    val tokens = text.map(doc => tokenize(doc))

    /**
      * **************************************train TF-IDF model*************************************
      */
    //set the dimensionality of TF-IDF vectors to 2^18
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)

    val tf = hashingTF.transform(tokens)
    //cache tf in memory
    tf.cache
    val v = tf.first.asInstanceOf[SV]
//    println(v.size)
//    // 262144
//    println(v.values.size)
//    // 706
//    println(v.values.take(10).toSeq)
//    // WrappedArray(1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 2.0, 1.0, 1.0)
//    println(v.indices.take(10).toSeq)
//    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val v2 = tfidf.first.asInstanceOf[SV]
//    println(v2.values.size)
//    // 706
//    println(v2.values.take(10).toSeq)
//    // WrappedArray(2.3869085659322193, 4.670445463955571, 6.561295835827856, 4.597686109673142,  ...
//    println(v2.indices.take(10).toSeq)
//    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)

    val hockeyText = rdd.filter { case (file, text) =>
      file.contains("hockey")
    }
    val hockeyTF = hockeyText.mapValues(doc =>
      hashingTF.transform(tokenize(doc)))
    val hockeyTfIdf = idf.transform(hockeyTF.map(_._2))

    //compute cosine similarity using Breeze
    import breeze.linalg._
    val hockey1 = hockeyTfIdf.sample(true, 0.1, 42).first.asInstanceOf[SV]
    val breeze1 = new SparseVector(hockey1.indices, hockey1.values, hockey1.size)
    val hockey2 = hockeyTfIdf.sample(true, 0.1, 43).first.asInstanceOf[SV]
    val breeze2 = new SparseVector(hockey2.indices, hockey2.values, hockey2.size)
    //忽略改错误
    val cosineSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))

    println(breeze1.dot(breeze2))
    println(cosineSim)

    /**
      * *********************************Word2Vec model********************************
      */

    val word2vec = new Word2Vec()
    word2vec.setSeed(42) // we do this to generate the same results each time
    val word2vecModel = word2vec.fit(tokens)

    //evaluate a few words
    word2vecModel.findSynonyms("hockey", 20).foreach(println)

    word2vecModel.findSynonyms("legislation", 20).foreach(println)
  }
}
