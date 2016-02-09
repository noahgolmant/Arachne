package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.arachne.LocalRunner;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.parser.ParseData;
import edu.uci.ics.crawler4j.parser.Parser;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by noah on 2/6/16.
 */
public class CrawlerBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(CrawlerBolt.class);

    private Map<String,String> stormConfig;
    private OutputCollector outputCollector;

    private PageFetcher pageFetcher;
    private Parser parser;

    public static Fields boltFields = new Fields("url", "date", "html");

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConfig = (Map<String,String>)map;
        this.outputCollector = outputCollector;

        CrawlConfig config = new CrawlConfig();
        config.setIncludeHttpsPages(true);
        pageFetcher = new PageFetcher(config);
        parser = new Parser(config);

    }

    public void execute(Tuple tuple) {
        /* get input tuple fields */
        String urlString = tuple.getStringByField("url");
        Date time = (Date)tuple.getValueByField("date");

        WebURL url = new WebURL();
        url.setURL(urlString);

        /* attempt to fetch the page */
        PageFetchResult pageFetchResult;
        try {
            pageFetchResult = pageFetcher.fetchPage(url);
            int statusCode = pageFetchResult.getStatusCode();

            /* check status code for retry/failure */
            if (statusCode != HttpStatus.SC_OK) {
                if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY) {
                    String movedTo = pageFetchResult.getMovedToUrl();
                    if (movedTo != null) {
                        // feed back new url to stream
                        logger.info("URL moved, from {} to {}", url, movedTo);
                        outputCollector.emit(LocalRunner.FILTER_STREAM,
                                new Values(movedTo, Calendar.getInstance().getTime()));
                    } else {
                        logger.info("URL moved, don't know where: {}", url);
                    }
                    outputCollector.ack(tuple);
                    return;
                } else if (statusCode == HttpStatus.SC_REQUEST_TIMEOUT
                            || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
                            || statusCode == HttpStatus.SC_BAD_GATEWAY) {
                    logger.info("request failed, retrying for: {}", url);
                    outputCollector.emit(LocalRunner.FILTER_STREAM,
                            new Values(url, Calendar.getInstance().getTime()));
                } else {
                    logger.info("{} status was {}", url, statusCode);
                }
            }

            /* check for redirect */
            if (!pageFetchResult.getFetchedUrl().equals(url.getURL())) {
                logger.info("redirected from {} to {}", url, pageFetchResult.getFetchedUrl());
            }

            /* fetch the HTML response and update the domain access cache */
            Page page = new Page(url);
            boolean fetched = pageFetchResult.fetchContent(page);
            FilterBolt.domainTracker.put(url.getDomain(), System.currentTimeMillis());
            if (!fetched) {
                logger.info("did not fetch/parse page: {}", url);
                outputCollector.ack(tuple);
                return;
            }

            /* finally parse the HTML data from the page */
            parser.parse(page, url.getURL());
            ParseData parseData = page.getParseData();
            if (!(parseData instanceof HtmlParseData)) {
                logger.info("parse data not HTML format for {}", url);
                outputCollector.ack(tuple);
                return;
            }

            /* get the raw html */
            HtmlParseData htmlParseData = (HtmlParseData)parseData;

            /* TODO get content size. ByteStream not available on getContent() call */
            /* store the page download size for logging analysis on DustBuster */
            /*int downloadSizeInBytes = ByteStreams.toByteArray(pageFetchResult.getEntity().getContent()).length;
            logger.info("Size in bytes for {} is: {}", url, downloadSizeInBytes);*/
            String htmlText = htmlParseData.getHtml();

            /* emit the URL's html to the article text extraction bolt */
            outputCollector.emit(LocalRunner.CRAWLER_STREAM, new Values(urlString, time, htmlText));
            logger.info("URL HTML: {}", htmlText);
            outputCollector.ack(tuple);


        } catch(Exception e) {
            logger.error(e.getMessage() + " while processing {}", url);
            outputCollector.fail(tuple);
            outputCollector.reportError(e);
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(LocalRunner.CRAWLER_STREAM, boltFields);
    }
}
