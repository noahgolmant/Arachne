package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.arachne.LocalRunner;
import com.google.common.collect.MapMaker;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
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

    private static final Logger logger = LoggerFactory.getLogger(FilterBolt.class);

    Map<String,String> stormConfig;
    OutputCollector    outputCollector;
    public static final Fields boltFields = new Fields("url", "date");

    /* Last time we updated the DustBuster ruleset */
    private static long lastDustBusterUpdate;
    private static final long DUSTBUSTER_UPDATE_PERIOD = 10 * 60 * 1000; /* once every ten minutes */
    private static final int MAX_DUSTBUSTER_ITERATIONS = 100;
    private static final String DUSTBUSTER_FILTERED = "--filtered--";

    /* Types of links and files we don't want to crawl */
    private static Pattern FILTERS[] = new Pattern[] {
            Pattern.compile("\\.(ico|css|sit|eps|wmf|zip|ppt|mpg|xls|gz|rpm|tgz|mov|exe|bmp|js)$"),
            //Pattern.compile("[\\?\\*\\!\\@\\=]"),
            Pattern.compile("^(file|ftp|mailto):"),
            //Pattern.compile(".*(/[^/]+)/[^/]+\1/[^/]+\1/")
    };

    /* Robots.txt checker */
    private RobotstxtServer robotstxtServer;

    /* Track last time we hit a given domain */
    transient static final ConcurrentMap<String, Long> domainTracker
            = new MapMaker().makeMap();
    private static long domainDelayTime;
    private static final long DEFAULT_DOMAIN_DELAY_TIME = 1000;

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
        this.stormConfig = (Map<String,String>)map;
        this.outputCollector = outputCollector;

        /* get configured domain check delay time */
        try {
            domainDelayTime = Integer.valueOf(stormConfig.get("domain.delay"));
        } catch (Exception e) {
            logger.warn("Invalid value for config field: domain.delay");
            domainDelayTime = DEFAULT_DOMAIN_DELAY_TIME;
        }

        /* setup crawler4j robots.txt check */
        CrawlConfig config = new CrawlConfig();
        config.setIncludeHttpsPages(true);
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
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
        Date feedDate = (Date)tuple.getValueByField("date");

        /* get canonicalized URL */
        WebURL url = new WebURL();
        url.setURL(URLCanonicalizer.getCanonicalURL(urlString));

        /* apply extension filters */
        if (extensionFilter(url)) {
            logger.info("Filtered by extension: {}", url);
            outputCollector.ack(tuple);
            return;
        }

        /* check robots.txt for politeness */
        if (!robotstxtServer.allows(url)) {
            logger.warn("Robots.txt denied access to: {}", url);
            outputCollector.ack(tuple);
            return;
        }

        /* check for domain access time limit */
        Long lastAccessTime = domainTracker.get(url.getDomain());
        long now = System.currentTimeMillis();
        if (lastAccessTime != null && now - lastAccessTime < domainDelayTime) {
            long throttleTime = domainDelayTime - (now - lastAccessTime);
            logger.warn("Throttling access to: {}. Sleeping for {} ms.", url, throttleTime);
            Utils.sleep(throttleTime);
        }

        /* access and apply DustBuster rules */
        /*url = dustbusterCanonicalizer(url);
        if (url.getURL().equals(DUSTBUSTER_FILTERED)) {
            logger.info("Filtered by DustBuster: {}", url);
            outputCollector.ack(tuple);
            return;
        }*/

        /* send to processing bolt */
        outputCollector.emit(LocalRunner.FILTER_STREAM, new Values(url.getURL(), feedDate));
        outputCollector.ack(tuple);

    }

    private static void updateDustBusterRuleset() {
        /* TODO access cassandra db rule schema */
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(LocalRunner.FILTER_STREAM, boltFields);
    }
}
