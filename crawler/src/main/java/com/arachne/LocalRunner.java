package com.arachne;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.arachne.bolts.CrawlerBolt;
import com.arachne.bolts.ExtractionBolt;
import com.arachne.bolts.FilterBolt;
import com.arachne.spouts.URLSpout;

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

        //Load properties file
        /*Properties props = new Properties();
        try {
            InputStream is = LocalRunner.class.getClassLoader().getResourceAsStream("scraper.properties");

            if (is == null)
                throw new RuntimeException("Classpath missing scraper.properties file");

            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

        Config conf = new Config();
        conf.setDebug(true);

        //Copy properies to storm config
        /*for (String name : props.stringPropertyNames()) {
            conf.put(name, props.getProperty(name));
        }*/

        conf.setMaxTaskParallelism(Runtime.getRuntime().availableProcessors());
        conf.setDebug(false);

        cluster.submitTopology("arachne-crawler", conf, createTopology());

        //Utils.sleep(10000);
        //cluster.killTopology("arachne-crawler");
        //cluster.shutdown();
    }

    static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("url_spout", new URLSpout(), 1);

        builder.setBolt("filter_bolt", new FilterBolt(), 2)
                .shuffleGrouping("url_spout");

        builder.setBolt("crawler_bolt", new CrawlerBolt(), 3)
                .shuffleGrouping("filter_bolt", LocalRunner.FILTER_STREAM);

        builder.setBolt("extraction_bolt", new ExtractionBolt(), 3)
                .shuffleGrouping("crawler_bolt", LocalRunner.CRAWLER_STREAM);

        return builder.createTopology();
    }
}
