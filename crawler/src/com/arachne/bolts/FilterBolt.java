package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Applies a series of filters to an input URL tuple
 * before processing for the rest of the crawler.
 * Applies:
 *  - file extension filter
 *  - DUST (duplicate URL with similar text) filter
 *  - politeness check with robots.txt
 *  - politeness check with domain access time throttling
 */
public class FilterBolt extends BaseRichBolt {

    Map<String,String> stormConfig;
    OutputCollector    outputCollector;
    public static final Fields boltFields = new Fields("url", "canonical_url", "start_time");

    /* Last time we updated the DustBuster ruleset */
    private static long lastDustBusterUpdate;
    private static final long DUSTBUSTER_UPDATE_PERIOD = 10 * 60 * 1000; /* once every ten minutes */
    private static final int MAX_DUSTBUSTER_ITERATIONS = 100;
    private static final String DUSTBUSTER_FILTERED = "--filtered--";

    /* Types of links and files we don't want to crawl */
    private static Pattern FILTERS[] = new Pattern[] {
            Pattern.compile("\\.(ico|css|sit|eps|wmf|zip|ppt|mpg|xls|gz|rpm|tgz|mov|exe|bmp|js)$"),
            Pattern.compile("[\\?\\*\\!\\@\\=]"),
            Pattern.compile("^(file|ftp|mailto):"),
            Pattern.compile(".*(/[^/]+)/[^/]+\1/[^/]+\1/")
    };

    /* Robots.txt checker */
    RobotstxtConfig robotstxtConfig;

    /**
     * Class representing a DUST rule determined by some
     * string transformation alpha |---> beta.
     *
     * e.g. for a rule (alpha = "index.html", beta = "")
     * the url "website.com/index.html" becomes "website.com/"
     */
    private class DustRule {
        private String alpha;
        private String beta;

        DustRule(String alpha, String beta) {
            this.alpha = alpha;
            this.beta = beta;
        }

        public String applyRule(String url) {
            int alphaIndex = url.indexOf(alpha);
            if (alphaIndex == -1)
                return url;
            return url.replaceAll(alpha, beta);
        }
    }

    private static DustRule dustRules[];


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConfig = map;
        this.outputCollector = outputCollector;
    }

    private boolean extensionFilter(WebURL url) {
        String currURL = url.getURL();
        for (Pattern pattern : FILTERS) {
            if (pattern.matcher(currURL).find())
                return true;
        }
        return false;
    }

    private WebURL dustbusterCanonicalizer(WebURL url) {
        String currURL = url.getURL();

        /* check if we need to update rule list */
        long currentTime = System.currentTimeMillis();
        if (lastDustBusterUpdate - currentTime > DUSTBUSTER_UPDATE_PERIOD) {
            FilterBolt.updateDustBusterRuleset();
            lastDustBusterUpdate = System.currentTimeMillis();
        }

        String preFilter = currURL;
        String postFilter = "";

        /* apply dust rules until the string does not change or we
           reached a fixed maximum number of iterations
         */
        for (int i = 0; i < MAX_DUSTBUSTER_ITERATIONS; i++) {
            preFilter = postFilter;
            postFilter = dustRules[i % dustRules.length].applyRule(preFilter);
            if (postFilter.equals(preFilter))
                break;
        }

        url.setURL(postFilter);
        return url;
    }

    public void execute(Tuple tuple) {
        String urlString = tuple.getStringByField("url");

        /* get canonicalized URL */
        WebURL url = new WebURL();
        url.setURL(URLCanonicalizer.getCanonicalURL(urlString));

        /* apply extension filters */
        if (extensionFilter(url)) {
            /* TODO log filtered URL */
            outputCollector.ack(tuple);
            return;
        }

        /* check robots.txt for politeness */

        /* access and apply DustBuster rules */
        url = dustbusterCanonicalizer(url);
        if (url.getURL().equals(DUSTBUSTER_FILTERED)) {
            /* TODO log dustbuster filtered url */
            outputCollector.ack(tuple);
            return;
        }

        /* send to processing bolt */
    }

    private static void updateDustBusterRuleset() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
