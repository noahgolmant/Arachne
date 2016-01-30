import org.apache.spark.SparkContext
import scala.math.min
import collections.mutable.HashMap
import scala.util.control.Breaks._

/**
  *
  */
object DustBuster {

    /**
     * Detect all likely rules where each rule is defined as a transformation
     * a |---> b are some substrings of a URL.
     */
    def detectLikelyRules(urlData:RDD[String]) : RDD[Tuple2[String, Integer]] = {

        val MaximumSubstringSize = 12

        /* Maps string to tuples where each tuple contains
        the substring, the text before that substr, the
        text after that substr and (*TODO*) the ratio
        of the size_range to the doc_sketch for the URL */
        def possibleSubstrings(line:String) : Iterable[Any] = {
            val len = min(line.length() - 1, MaximumSubstringSize)
            for {
                i <- 1 to len
                j <- 0 to len-i
            } yield {
                /* for each substring in line of length i, get prefix and suffix */
                val substr = line.substring(j, j+i)
                val prefix = line.substring(0, j)
                val suffix = line.substring(j+i, len)
                (substr, prefix, suffix)
            }
        }

        val BucketOverflowSize = 1000
        val MaxSubstringLengthDiff = 4

        def filterBuckets(envelope:Tuple2[String,String], substrings:Iterable[String]) : Iterable[Any] = {
        
            def likelySimilar(substr1: String, substr2: String) : Boolean = {
                /* TODO use size ratio or document sketch */
                /* Or use hamming/levenshtein distance here */
                return Math.abs(substr1.length() - substr2.length()) < MaxSubstringLengthDiff
            }
            if (substrings.size == 1 || substrings.size > BucketOverflowSize)
                return None
        
            val (prefix, suffix) = envelope
            val substringsLst = new Array[String](substrings.size)
            substrings.copyToArray(substringsLst)
            for {
                substr1 <- substringsLst
                substr2 <- substringsLst
            } yield {
                if (likelySimilar(substr1, substr2))
                    ((substr1,substr2),(prefix,suffix))
            }
        }

        return urlData.flatMap(possibleSubstrings) /* get all possible substrings for each url */
                      /* create buckets based on unique prefix suffix pairs */
                      .map((substr,prefix,suffix) => ((prefix, suffix), substr)).groupByKey()
                      /* create potential rules and group by substring pairs (a,b) where one rule = a |--> b transform */
                      .flatMap(filterBucket).filter(_.nonEmpty).groupByKey()
                      /* calculate rule support and sort by best rule */
                      .mapValues(envelopes => envelopes.size)
                      .sortBy(_._2) /* sort by second element of (rule, support) tuple i.e. support count */
    }

    def eliminateRedundantRules(likelyRules:RDD[Tuples2[Tuple2[String,String],Integer]]) : RDD[Tuple2[String,String]] = {

        val MaxWindowSize = 50 /* tunable size of "window" of rules to compare one rule to */
        val MaxRelativeDeficiency = 6 /* tunable max support difference between rules in same window */
        val MaxAbsoluteDeficiency = 10

        var rules = likelyRules.collect() /* pray we don't overload the driver memory */
        
        /* determines if one rule refines another e.g. the support of the first is a subset of the support of the second.
         * For rule1 = a --> b and rule 2 = a' --> b', checks if a is a substring of a', replaces a by b and check if the outcome is b'.
         */
        def refines(rule1:Tuple2[String,String], rule2:Tuple2[String,String]) : Boolean = {
            if (!(rule2[0] contains rule1[0]))
                return false

            var lastIndex = 0
            val a = rule1[0]
            val b = rule1[1]
            val a_prime = rule2[0]
            val b_prime = rule2[1]
            while (lastIndex != -1) {
                lastIndex = a_prime.indexOf(a, lastIndex)
                if (lastIndex != -1) {
                    val withReplacedA = (a_prime substring lastIndex) concat b concat (a_prime substring (lastIndex+a.length()))
                    if (withReplacedA equals b_prime)
                        return true
                    lastIndex += rule1[0].length()
                }
            }
            return false
        }

        
        var eliminatedRules = new HashMap[String, Boolean]() { override def default(key:Boolean) = false
        for (i <- 0 to rules.length()) {
            var rule = rules[i]
            if (!eliminatedRules(rule)) {
                breakable { for (j <- 1 to min(MaxWindowSize, rules.length())) {
                    if (rule[1] - rules[i+j][1] > max(MaxRelativeDeficiency - rule[1], MaxAbsoluteDeficiency))
                        break
                    /* check if R[i] refines R[i+j] or R[i+j] refies R[i].
                     * Refines if support of one is a subset of the support of the other.
                     * (TODO check if this should compare number of shared URLs rather than support size) */
                    if (refines(rule[0], rules[i+j][0]))
                        eliminatedRules += rules[i+j]
                    else if (refines(rule[i+j][0], rule[0])) {
                        eliminatedRules += rules[i]
                        break
                    }
                } }
            }
        }
        return sc.parallelize(rules.filter(!eliminatedRules(_))) /* re-parallelize after dealing with windowed filtering that requires indices */ 
    }

    def main(args:Array[String]) = {
        val sc = new SparkContext()
        var urlData = sc.parallelize(args(0))
        urlData = urlData.map(line => "^" + line + "$") /* tokenize urls */
        
        var likelyRules = detectLikelyRules(urlData)
    }
}
