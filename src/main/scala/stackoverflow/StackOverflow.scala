package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {


    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
   // val lines   = sc.textFile("src/main/resources/stackoverflow/test.csv")
    val raw     = rawPostings(lines)
    def printRawRDD: Unit ={
      raw.collect().foreach(p =>println(p))
    }
   // printRawRDD
    val grouped = groupedPostings(raw)
    def printGroupedRDD: Unit ={
      grouped.collect().foreach(p =>println(p))
    }
   // printGroupedRDD

    val scoring = scoredPostings(grouped)
    def printScoredRDD: Unit ={
      scoring.collect().foreach(p =>println(p))
    }
   // printScoredRDD

    val vectP = vectorPostings(scoring)
    def printVectorsRDD: Unit ={
      vectP.collect().foreach(p =>println(p))
    }
    //printVectorsRDD
    println("the number of vectors to treat are: ", vectP.count()) // it is correct
    //    assert(vectP.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
    val SvectP = sampleVectors(vectP)
    def printSVectorsRDD: Unit ={
      SvectP.foreach(svect => println("componants of Svact(i): ", svect._1,":",svect._2 ))
    }
    //printSVectorsRDD


    val means   = kmeans(SvectP,vectP,1,true)
    def printMeans: Unit ={
      means.foreach(mn => println("componants of Svact(i): ", mn._1,":",mn._2 ))
    }
    printMeans

    //val results = clusterResults(means, SvectP)
    //printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000

  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45
  //def kmeansKernels = 15

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
    })



    /** Group the questions and answers together */
    def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
      val questions = postings.filter(p => p.postingType == 1).map(p => (p.id,p))  //p.id.get ?
      val answers = postings.filter(p => (p.postingType == 2 && p.parentId.isDefined)).map(p => (p.parentId.get,p))
      val joined_RDD = questions.join(answers)   //to check what is the common argument to join on here
      // val res= joined_RDD.reduce((a:Int,(b,c)) =>())
      joined_RDD.groupByKey()
      //to see reduceByKey how to do it
      //should use persist or cache?
    }


      /** Compute the maximum score for each posting */
      def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

        def answerHighScore(as: Array[Answer]): HighScore = {
          var highScore = 0
              var i = 0
              while (i < as.length) {
                val score = as(i).score
                    if (score > highScore)
                      highScore = score
                      i += 1
              }
          highScore
        }

        //grouped.flatMap(_._2).groupByKey().mapValues(answerHighScore())
        //grouped.flatMap(_._2).groupByKey().mapValues(v =>answerHighScore(v.toArray))
        val scores = grouped.map {
          case (id, tupleOfPostings_Q) => {
          val the_question = tupleOfPostings_Q.map(x => x._1).toArray
          val the_score = answerHighScore(tupleOfPostings_Q.map(x => x._2).toArray)
          (the_question(0), the_score)
        }
        }
         scores
        }


        /** Compute the vectors for the kmeans */
        def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
          /** Return optional index of first language that occurs in `tags`. */
          def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
            if (tag.isEmpty) None
            else if (ls.isEmpty) None
            else if (tag.get == ls.head) Some(0) // index: 0
            else {
              val tmp = firstLangInTag(tag, ls.tail)
              tmp match {
                case None => None
                case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
              }
            }
          }

          /*scored.map( qh => (
                              {
                                val myLangIndex:Option[Int] = firstLangInTag(qh._1.tags,langs)
                                //val i = a.get
                                //val myLangIndex = firstLangInTag(qh._1.tags,langs)
                                if (myLangIndex!= None) myLangIndex.get * langSpread //myLangIndex.get is because we are agetting an int from an option ???
                                else 0
                              }
                              ,qh._2
                            )
                    ) //is collect necessary in here? why does the compilation worked out when I have inserted it*/

          val the_vectors= scored.map { t =>
            val index = firstLangInTag(t._1.tags,langs).get
            (index * langSpread , t._2)
          }
          the_vectors
        }


         /** Sample the vectors */
         def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

           assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
           val perLang = kmeansKernels / langs.length

           // http://en.wikipedia.org/wiki/Reservoir_sampling
           def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
             val res = new Array[Int](size)
             val rnd = new util.Random(lang)


             for (i <- 0 until size) {

               assert(iter.hasNext, s"iterator must have at least $size elements")
               res(i) = iter.next
             }

             var i = size.toLong
             while (iter.hasNext) {
               val elt = iter.next
               val j = math.abs(rnd.nextLong) % i
               if (j < size)
                 res(j.toInt) = elt
               i += 1
             }

             res
           }

           val res =
             if (langSpread < 500)
               // sample the space regardless of the language
               vectors.takeSample(false, kmeansKernels, 42)
             else
               // sample the space uniformly from each language partition
               vectors.groupByKey.flatMap({
                 case (lang, vector) => reservoirSampling(lang, vector.toIterator, perLang).map((lang, _)) //vector instead of vectors
               }).collect()
           //perLang is 3, vector.toIterator size is one, so not sure of this line????
           res.foreach(x=>println(x))
           assert(res.length == kmeansKernels, res.length)
           res
         }


         //
         //
         //  Kmeans method:
         //
         //

         /** Main kmeans computation */
         @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
          // val newMeans = means.clone() // you need to compute newMeans
/*
           // too slow
           vectors
             .map(
               vector => (findClosest(vector, means), vector)
             )
             .groupByKey()
             .mapValues(averageVectors)
             .collect()
             .foreach(pair => {
               newMeans.update(pair._1, pair._2)
             })*/

           val newMeans = vectors
             .map(
               vector => (findClosest(vector, means), vector)
             )
             .groupByKey()
             .mapValues(second2t => averageVectors(second2t))
             .collect()
             .map(_._2)
            // .foreach(pair => {
              // newMeans.update(pair._1, pair._2)
             //})


           val distance = euclideanDistance(means, newMeans)

           if (debug) {
             println(s"""Iteration: $iter
                        |  * current distance: $distance
                        |  * desired distance: $kmeansEta
                        |  * means:""".stripMargin)
             for (idx <- 0 until kmeansKernels)
             println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
                     f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
           }

           if (converged(distance))
             newMeans
           else if (iter < kmeansMaxIterations)
             kmeans(newMeans, vectors, iter + 1, debug)
           else {
             if (debug) {
               println("Reached max iterations!")
             }
             newMeans
           }
         }




         //
         //
         //  Kmeans utilities:
         //
         //

         /** Decide whether the kmeans clustering converged */
         def converged(distance: Double) =
           distance < kmeansEta


         /** Return the euclidean distance between two points */
         def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
           val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
           val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
           part1 + part2
         }

         /** Return the euclidean distance between two points */
         def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
           assert(a1.length == a2.length)
           var sum = 0d
           var idx = 0
           while(idx < a1.length) {
             sum += euclideanDistance(a1(idx), a2(idx))
             idx += 1
           }
           sum
         }

         /** Return the closest point */
         def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
           var bestIndex = 0
           var closest = Double.PositiveInfinity
           for (i <- 0 until centers.length) {
             val tempDist = euclideanDistance(p, centers(i))
             if (tempDist < closest) {
               closest = tempDist
               bestIndex = i
             }
           }
           bestIndex
         }


         /** Average the vectors */
         def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
           val iter = ps.iterator
           var count = 0
           var comp1: Long = 0
           var comp2: Long = 0
           while (iter.hasNext) {
             val item = iter.next
             comp1 += item._1
             comp2 += item._2
             count += 1
           }
           ((comp1 / count).toInt, (comp2 / count).toInt)
         }




         //
         //
         //  Displaying results:
         //
         //
         def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
           val closest = vectors.map(p => (findClosest(p, means), p))
           val closestGrouped = closest.groupByKey() // for every key (equivalent to a mean) we have an iterable of (langindex,Highscore)

           val median = closestGrouped.mapValues { vs =>

             val grouped: Map[Int, Int] = vs
               .map(_._1 / langSpread)
               .groupBy(identity)
               .mapValues(_.size)


             val maxLangIndex = grouped.maxBy(_._2)._1



             // most common language in the cluster
             val langLabel: String = langs(maxLangIndex)

             // percent of the questions in the most common language
             val langPercent: Double = grouped(maxLangIndex) * 100.0d / vs.size

             val clusterSize: Int = vs.size

             // All for the median
             val sortedScores = vs.map(_._2).toList.sorted //highscore of the cluster points to list then sorted
             val middle = clusterSize / 2
             val medianScore: Int = if(clusterSize % 2 == 0) (sortedScores(middle-1) + sortedScores(middle)) / 2 else sortedScores(middle)


             (langLabel, langPercent, clusterSize, medianScore)
           }

           median.collect().map(_._2).sortBy(_._4)
         }

         def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
           println("Resulting clusters:")
           println("  Score  Dominant language (%percent)  Questions")
           println("================================================")
           for ((lang, percent, size, score) <- results)
             println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
         }
}
