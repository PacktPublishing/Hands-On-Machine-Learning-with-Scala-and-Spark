package pl.tomekl007.spark.book

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import pl.tomekl007.spark.utils.InputCleanUtil

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SemanticAnalysis {

  //tf idf
  def termDocWeight(termFrequencyInDoc: Int, totalTermsInDoc: Int, termFreqInCorpus: Int, totalDocs: Int): Double = {
    val tf = termFrequencyInDoc.toDouble / totalTermsInDoc
    val docFreq = totalDocs.toDouble / termFreqInCorpus
    val idf = math.log(docFreq)
    tf * idf
  }

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }


  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
  : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && InputCleanUtil.containsOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    InputCleanUtil.clearWords(lemmas)
  }


  def apply(plainText: RDD[String], stopWords: Set[String]) = {

    val lemmatized: RDD[Seq[String]] = lemmatize(plainText, stopWords)

    val docTermFreqs = getTermsFrequencyPerDoc(lemmatized)
    docTermFreqs.cache()
    //    docTermFreqs.foreach(println)

    val docFreqs = documentFrequenciesDistributed(docTermFreqs, 50000)
    //    println("Number of terms: " + docFreqs.length)
    //    println(docFreqs.toList)


    val docIds = docTermFreqs.flatMap(x => x.keySet).zipWithUniqueId().map(_.swap).collectAsMap()
    val idsAndDocs = docIds.map(_.swap)
    val numDocs = idsAndDocs.size
    //    println("number of docs : " + numDocs)

    val idfs = calculateIdf(docFreqs, numDocs)
    //    println(idfs)
    val termsWithIds = idfs.keys.zipWithIndex.toMap
    val idsAndTerms = termsWithIds.map(_.swap)
    //    println(termsWithIds)
    val vect = toVect(docTermFreqs, idfs, termsWithIds)


    val svd: SingularValueDecomposition[RowMatrix, Matrix] = computeSVD(vect)
    println(svd)

    val topConceptTerms = topTermsInTopConcepts(svd, 4, 10, idsAndTerms)
    val topConceptDocs = topDocsInTopConcepts(svd, 4, 10, docIds.toMap)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }
    /*
Concept terms: devenir, jamais, femmes, semble, mondiale, hui, moin, background, mettre, mesure
Concept docs: kosztuje, jesli, głównie, chcesz, odebrano, autora, krzywdę, problemów, oczywiscie, promocji

Concept terms: img, zwycięzca, zen, pierdolne, tłumem, kradnij, uzupełniając, prubę, czułeś, kowalik
Concept docs: podchodzeniu, wiecej, objąć, zjazdów, mojego, grubyilysy, siebie, jedziesz, sznury, osamę

Concept terms: gratulacje, img, szacuneczek, fantastyczna, brawa, zajebiscie, mechanior, udana, blanka, podziękowania
Concept docs: idzie, wpadnie, moc, ostatnio, epitety, oszczednosci, zakopane, kulturze, komuś, napisał

Concept terms: url, napisał, jersey, dzięki, info, sie, temacie, the, pozdrawiam, rip
Concept docs: wybacz, pilka, środowiska, itp, zmusza, reverso, oczywiście, ciekawa, napisał, chodzi


     */




    findSimilarities(docIds, idsAndDocs, termsWithIds, idsAndTerms, svd)


  }


  def findSimilarities(docIds: collection.Map[Long, String], idsAndDocs: collection.Map[String, Long], termsWithIds: Map[String, Int], idsAndTerms: Map[Int, String], svd: SingularValueDecomposition[RowMatrix, Matrix]): Unit = {


    while (true) {
      val userinput = readLine //should be type:term eg t-t:góry
      try {
        val input = userinput.split(":")
        if (input.size != 2) {
          println("wrong format")
        }else {
          val arg = input(1)
          input(0) match {
            case "t-t" => {
              //term-term
              val VS = TopTerms.multiplyByDiagonalMatrix(svd.V, svd.s)
              val normalizedVS = TopTerms.rowsNormalized(VS)
              TopTerms.printTopTermsForTerm(normalizedVS, arg, termsWithIds, idsAndTerms)
            }
            case "d-d" => {
              //doc-doc
              val US = TopTerms.multiplyByDiagonalMatrix(svd.U, svd.s)
              val normalizedUS = TopTerms.rowsNormalized(US)

              TopTerms.printTopDocsForDoc(normalizedUS, arg, idsAndDocs.toMap, docIds.toMap)
            }
            case "t-d" => {
              //term-doc
              val US = TopTerms.multiplyByDiagonalMatrix(svd.U, svd.s)
              val normalizedUS = TopTerms.rowsNormalized(US)
              TopTerms.printTopDocsForTerm(normalizedUS, svd.V, arg, termsWithIds, docIds.toMap)
            }
            case _ => {
              println("operation not found")
            }
          }
        }
      } catch {
        case e: IllegalStateException => println(e)
        case ex: NoSuchElementException => println(ex)
      }

    }
  }

  def computeSVD(vect: RDD[Vector]): SingularValueDecomposition[RowMatrix, Matrix] = {
    vect.cache()
    val mat = new RowMatrix(vect)
    val k = 5
    val svd = mat.computeSVD(k, computeU = true)
    svd
  }

  def calculateIdf(docFreqs: Array[(String, Int)], numDocs: Long): Map[String, Double] = {
    docFreqs.map {
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }.toMap
  }

  def getTermsFrequencyPerDoc(lemmatized: RDD[Seq[String]]): RDD[mutable.HashMap[String, Int]] = {
    lemmatized.map(terms => {
      val termFreqs = terms.foldLeft(new mutable.HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      termFreqs
    })
  }

  def lemmatize(plainText: RDD[String], stopWords: Set[String]): RDD[Seq[String]] = {
    val lemmatized = plainText.repartition(16).mapPartitions(iter => {
      val pipeline = createNLPPipeline()
      iter.map { case (text) => plainTextToLemmas(text, stopWords, pipeline) }
    })
    //    lemmatized.take(10).foreach(_.foreach(println))
    lemmatized
  }

  def documentFrequenciesDistributed(docTermFreqs: RDD[mutable.HashMap[String, Int]], numTerms: Int)
  : Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _, 15)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def toVect(docTermFreqs: RDD[mutable.HashMap[String, Int]], idfs: Map[String, Double], termsWithIds: Map[String, Int]) = {

    docTermFreqs.map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter { case (term, freq) =>
        termsWithIds.containsKey(term)
      }.map {
        case (term, freq) => (termsWithIds(term), idfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(termsWithIds.size, termScores)
    })
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map { case (score, id) => (termIds(id), score) }
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                           numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map { case (score, id) => (docIds(id), score) }
    }
    topDocs
  }

}
