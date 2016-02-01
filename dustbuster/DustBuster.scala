import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.TraversableOnce
import scala.math.{max, min}
import scala.util.control.Breaks._

/**
  *
  */
object DustBuster {

    type Rule = (String,String)
    type Envelope = (String,String)

    val sc = new SparkContext()

    /**
      * Detect all likely rules where each rule is defined as a transformation
      * a |---> b are some substrings of a URL.
      */
    def detectLikelyRules(urlData: RDD[String]): RDD[(Rule,Int)] = {

        val MaximumSubstringSize = 12

        /* Maps string to tuples where each tuple contains
        the substring, the text before that substr, the
        text after that substr and (*TODO*) the ratio
        of the size_range to the doc_sketch for the URL */
        def possibleSubstrings(line: String): TraversableOnce[(Envelope, String)] = {
            val len = min(line.length - 1, MaximumSubstringSize)
            for {
                i <- 1 to len
                j <- 0 to len - i
            } yield {
                /* for each substring in line of length i, get prefix and suffix */
                val substr = line substring (j, j + i)
                val prefix = line substring(0, j)
                val suffix = line substring(j + i, len)
                val envelope = (line.substring(0, j), line.substring(j+i, len))
                (envelope, substr)
                //(substr, prefix, suffix)
            }
        }

        val BucketOverflowSize = 1000
        val MaxSubstringLengthDiff = 4

        def filterBuckets(envelopeWithSubstrings: (Envelope, Iterable[String])): TraversableOnce[(Rule, Envelope)] = {
            val envelope = envelopeWithSubstrings._1
            val substrings = envelopeWithSubstrings._2

            def likelySimilar(substr1: String, substr2: String): Boolean = {
                /* TODO use size ratio or document sketch */
                Math.abs(substr1.length - substr2.length) < MaxSubstringLengthDiff
            }

            //if (substrings.size == 1 || substrings.size > BucketOverflowSize)
            //    return List((("",""), envelope))

            val substringsLst = new Array[String](substrings.size)
            substrings.copyToArray(substringsLst)
            for {
                substr1 <- substringsLst
                substr2 <- substringsLst
                if (likelySimilar(substr1, substr2))
            } yield {
                val rule = (substr1,substr2)
                (rule, envelope)
            }
        }

        return urlData
            .flatMap(possibleSubstrings) /* get all possible substrings for each url */
            .groupByKey()                /* create buckets based on unique envelope */
            .flatMap(filterBuckets)      /* create potential rules */
            .groupByKey()                /* group support of rules that are the same together */
            .mapValues(envelopes => envelopes.size) /* get the rule support, i.e. how many urls follow it */
            .sortBy(_._2)                /* sort by the most supported rules */
    }

    def eliminateRedundantRules(likelyRules: RDD[(Rule,Int)]): RDD[Rule] = {

        val MaxWindowSize = 50 /* tunable size of "window" of rules to compare one rule to */
        val MaxRelativeDeficiency = 6 /* tunable max support difference between rules in same window */
        val MaxAbsoluteDeficiency = 10

        val rules = likelyRules.collect();

        /* determines if one rule refines another e.g. the support of the first is a subset of the support of the second.
         * For rule1 = a --> b and rule 2 = a' --> b', checks if a is a substring of a', replaces a by b and check if the outcome is b'.
         */
        def refines(rule1: Rule, rule2: Rule): Boolean = {
            if (!(rule2._1 contains rule1._1))
                return false

            var lastIndex = 0
            val a = rule1._1
            val b = rule1._2
            val a_prime = rule2._1
            val b_prime = rule2._2
            while (lastIndex != -1) {
                lastIndex = a_prime.indexOf(a, lastIndex)
                if (lastIndex != -1) {
                    val withReplacedA = (a_prime substring lastIndex) concat b concat (a_prime substring (lastIndex + a.length()))
                    if (withReplacedA equals b_prime)
                        return true
                    lastIndex += rule1._1.length()
                }
            }
            false
        }

        var eliminatedRules = Array[Rule]()
        for (i <- 0 to rules.length) {
            val rule = rules(i)._1
            val support = rules(i)._2
            if (!(eliminatedRules contains rule)) {
                breakable {
                    for (j <- 1 to min(MaxWindowSize, rules.length)) {
                        val windowRule = rules(i+j)._1
                        val windowSupport = rules(i+j)._2
                        if (support - windowSupport > max(MaxRelativeDeficiency - support, MaxAbsoluteDeficiency))
                            break()

                        /* check if R[i] refines R[i+j] or R[i+j] refies R[i].
                         * Refines if support of one is a subset of the support of the other.
                         * (TODO check if this should compare number of shared URLs rather than support size) */

                        if (refines(rule, windowRule))
                            eliminatedRules :+= windowRule
                        else if (refines(windowRule, rule)) {
                            eliminatedRules :+= rule
                            break()
                        }
                    }
                }
            }
        }



        sc.parallelize(rules.map(rule => rule._1).filter(!eliminatedRules.contains(_)))/* re-parallelize after dealing with windowed filtering that requires indices */
    }

    def main(args: Array[String]) = {
        var urlData = sc.textFile(args(0))
        urlData = urlData.map(line => "^" + line + "$") /* tokenize urls */
        var likelyRules = detectLikelyRules(urlData)
        var filteredRules = eliminateRedundantRules(likelyRules)
        filteredRules.take(10).foreach(println(_))
    }
}
