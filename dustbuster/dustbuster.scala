import org.apache.spark.SparkContext

object DustBuster {

    /* Maps string to tuples where each tuple contains
       the substring, the text before that substr, the
       text after that substr and (*TODO*) the ratio
       of the size_range to the doc_sketch for the URL */
    def possibleSubstrings(line:String) : List[Tuple3[String,String,String]] = {
        val len = line.length() - 1;
        var substrings = List[Tuple3[String,String,String]]();
        for ( i <- 1 to len) {
            /* for each substring in line of length i, get prefix and suffix */
            for ( j <- 0 to len-i) {
                val substr = line.substring(j, j+i);
                val prefix = line.substring(0, j);
                val suffix = line.substring(j+i, len);
                substrings = (substr, prefix, suffix) :: substrings;                   
            }
        }
        return substrings;
    }

    val BucketOverflowSize = 1000;
    val MaxSubstringLengthDiff = 4;

    def filterBuckets((prefix, suffix):Tuple2[String,String], substrings:Iterable[String]) = {
        
        def likelySimilar(substr1: String, substr2: String) {
            /* TODO use size ratio or document sketch */
            return Math.abs(substr1.length() - substr2.length()) < MaxSubstringLengthDiff;
        }
        
        if (substrings.size == 1 || substrings.size > BucketOverflowSize)
            return None;

        for ( substr1 <- substrings ) {
            for ( substr2 <- substrings) {
                if (likelySimilar(substr1, substr2))
                    yield (substr1, substr2)
            }
        }
    }

    def main(args:Array[String]) = {
        val sc = new SparkContext();
        val urlData = sc.parallelize(args(0));
        urlData = urlData.map(line => "^" + line + "$"); /* tokenize urls */
        /* collect possible substrings of each URL */
        val substringTable = urlData.flatMap(possibleSubstrings);
        /* group all substrings into buckets by (prefix, suffix) tuple */
        val buckets = substringTable
            .map((substr, prefix, suffix) => ((prefix, suffix), substr)
            .groupByKey();
           
        val instanceTable = buckets.map(filterBuckets).filter(_.nonEmpty);
    }
}