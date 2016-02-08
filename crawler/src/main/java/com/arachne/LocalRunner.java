package com.arachne;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by noah on 2/8/16.
 */
public class LocalRunner {

    public static String FILTER_STREAM     = "filter_stream";
    public static String EXTRACTION_STREAM = "extraction_stream";
    public static String URL_STREAM        = "url_stream";
    public static String CRAWLER_STREAM    = "crawler_stream";

    public static void main(String[] args) {
        LocalCluster cluster = new LocalCluster();

        /* todo load config properties */

    }

    static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        /* todo actually assign the bolts/spouts to the topology */
        return builder.createTopology();
    }
}
