package stackoverflow

/*
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }





  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }*/

/*
  test("rawPostings"){
    //raw.collect().take(10).foreach(println)

    //println(raw.toDebugString)

    assert(raw.count() > 2121822)
  }
*/

  /*test("groupedPostings"){
    grouped.take(10).foreach(println)

    println(grouped.toDebugString)

    assert(grouped.count() === 26075)
  }

  //scored RDD should have 2121822 entries
  test("scoredPostings"){
    scored.take(10).foreach(println)

    println(scored.toDebugString)

    assert(scored.count() === 26075)
    assert(scored.take(2)(1)._2 === 3)
  }

  test("vectorPostings"){
    vectors.take(10).foreach(println)

    println(vectors.toDebugString)

    assert(vectors.count() === 26075)
    assert(vectors.take(2)(0)._1 === 4 * testObject.langSpread)
  }*/

  /*test("kmeans"){
    println("K-means: " + means.mkString(" | "))
    assert(means(0) === (1,0))
  }

  test("results"){
    results(0)
    testObject.printResults(results)
    assert(results(0) ===  ("Java", 100, 1361, 0))
  }*/


//}
