import org.apache.spark.SparkContext
import scala.math.min

object DustBuster {

    def detectLikelyRules(urlData:RDD[String]) : RDD[Tuple2[String, Integer]] = {

        val MaximumSubstringSize = 12;

        /* Maps string to tuples where each tuple contains
        the substring, the text before that substr, the
        text after that substr and (*TODO*) the ratio
        of the size_range to the doc_sketch for the URL */
        def possibleSubstrings(line:String) : Iterable[Any] = {
            val len = min(line.length() - 1, MaximumSubstringSize);
            for {
                i <- 1 to len
                j <- 0 to len-i
            } yield {
                /* for each substring in line of length i, get prefix and suffix */
                val substr = line.substring(j, j+i);
                val prefix = line.substring(0, j);
                val suffix = line.substring(j+i, len);
                (substr, prefix, suffix);
            };
        }

        val BucketOverflowSize = 1000;
        val MaxSubstringLengthDiff = 4;

        def filterBuckets(envelope:Tuple2[String,String], substrings:Iterable[String]) : Iterable[Any] = {
        
            def likelySimilar(substr1: String, substr2: String) : Boolean = {
                /* TODO use size ratio or document sketch */
                /* Or use hamming/levenshtein distance here */
                return Math.abs(substr1.length() - substr2.length()) < MaxSubstringLengthDiff;
            }
            if (substrings.size == 1 || substrings.size > BucketOverflowSize)
                return None;
        
            val (prefix, suffix) = envelope;
            val substringsLst = new Array[String](substrings.size);
            substrings.copyToArray(substringsLst);
            for {
                substr1 <- substringsLst
                substr2 <- substringsLst
            } yield {
                if (likelySimilar(substr1, substr2))
                    ((substr1,substr2),(prefix,suffix))
            };
        }

        return urlData.flatMap(possibleSubstrings) /* get all possible substrings for each url */
                      /* create buckets based on unique prefix suffix pairs */
                      .map((substr,prefix,suffix) => ((prefix, suffix), substr)).groupByKey()
                      /* create potential rules and group by substring pairs (a,b) where one rule = a |--> b transform */
                      .flatMap(filterBucket).filter(_.nonEmpty).groupByKey()
                      /* calculate rule support and sort by best rule */
                      .mapValues(envelopes => envelopes.size)
                      .sortBy(_._2); /* sort by second element of (rule, support) tuple i.e. support count */
    }

    def main(args:Array[String]) = {
        val sc = new SparkContext();
        var urlData = sc.parallelize(args(0));
        urlData = urlData.map(line => "^" + line + "$"); /* tokenize urls */
        
        var likelyRules = detectLikelyRules(urlData);
    }
}
