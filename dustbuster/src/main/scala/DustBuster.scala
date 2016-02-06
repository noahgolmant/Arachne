import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.TraversableOnce
import scala.math.{max, min}
import scala.util.control.Breaks._

/**
  * Detects DUST urls - "duplicate URLS with similar text"
  * Uses string distance metrics, HTTP header info and
  * other log data to create a list of filtering rules that determine
  * if a new URL is a mirror/duplicate of a previously downloaded one
  * without actually comparing the HTML.
  */
object DustBuster {

    type Rule = (String,String)
    type Envelope = (String,String)

    var sc: SparkContext = null

    /**
      * Detect all likely rules where each rule is defined as a transformation
      * a |---> b are some substrings of a URL.
      */
    def detectLikelyRules(urlData: RDD[(String, Int)]): RDD[(Rule,Int)] = {

        val MaximumSubstringSize = 12

        /* Maps string to tuples where each tuple contains
        the substring, the text before that substr, the
        text after that substr and (*TODO*) the byte size
        range of urls that contain that substring*/
        def possibleSubstrings(lineWithSupport: (String, Int)): TraversableOnce[(Envelope, (String, Int))] = {
            val line = lineWithSupport._1
            val support = lineWithSupport._2

            val len = min(line.length - 1, MaximumSubstringSize)
            for {
                i <- 1 to len
                j <- 0 to len - i
            } yield {
                /* for each substring in line of length i, get prefix and suffix,
                 * which are the parts of the URL that occur before and after that substring */
                val substr = line substring (j, j + i)
                val envelope = (line.substring(0, j) /* prefix */, line.substring(j+i, len) /* suffix */)
                (envelope, (substr, support))
            }
        }

        val BucketOverflowSize = 1000
        //val MaxSubstringLengthDiff = 4
        val MaxPercentSizeDifference =  0.20

        def filterBuckets(envelopeWithSubstrings: (Envelope, Iterable[(String,Int)])): TraversableOnce[(Rule, Envelope)] = {
            val envelope = envelopeWithSubstrings._1
            val substrings = envelopeWithSubstrings._2

            /* compares support (2nd element of tuple) of each rule
            *  and makes sure it isn't a trivial transformation (x \--> x) */
            def likelySimilar(substr1: (String, Int), substr2: (String,Int)): Boolean = {
                !(substr1._1 equals substr2._1) &&
                Math.abs(substr1._2 - substr2._2) / substr1._2 < MaxPercentSizeDifference
            }

            if (substrings.size == 1 || substrings.size > BucketOverflowSize)
                return List((("",""), envelope)) /* return nothing if the rule is too vague */

            /* copy TraversableOnce to array to allow nested iteration over set */
            val substringsLst = new Array[(String,Int)](substrings.size)
            substrings.copyToArray(substringsLst)
            /* emit a rule if the two substrings describe a similar transformation */
            for {
                substr1 <- substringsLst
                substr2 <- substringsLst
                if likelySimilar(substr1, substr2)
            } yield {
                val rule = (substr1._1,substr2._1)
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

    /* eliminate potentially redundant rules based on
       whether or not one rule "refines" another, i.e. its
       support or structure is a subset of some other rule.
       TODO do not use RDD.collect() -- group the RDD into buckets
            to search over in place of windows.
     */
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

        /* iterate over all rules and see if there are very similar rules
           within a a specified window of indices
         */
        var eliminatedRules = Array[Rule]()
        for (i <- 0 to rules.length-1) {
            val rule = rules(i)._1
            val support = rules(i)._2
            if (!(eliminatedRules contains rule)) {
                breakable {
                    for (j <- 1 to min(MaxWindowSize, rules.length - i - 1)) {
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
        val conf = new SparkConf().setAppName("DustBuster")
        sc = new SparkContext(conf)

        if (args.length == 0) {
            println("Input URL list required")
            sys.exit(0)
        }

        /* map CSV to url, page-size tuples */
        /* CSV has url, page-size-in-bytes on each line */
        val textData = sc.textFile(args(0))
        def splitter = (line:String) => {
            val data = line.split(",");
            try {
                (data(0), data(1).toInt)
            } catch {
            case e: Exception => (data(0), 0)
            }
        }
        val urlData = textData.map(splitter).filter(pair => pair._2 > 0)
        val likelyRules = detectLikelyRules(urlData)
            .filter(tuple => !tuple._1.equals(("",""))) /* clear empty rules */
        val filteredRules = eliminateRedundantRules(likelyRules)
        filteredRules.sortBy(_._2).collect().foreach(println(_))
    }
}
