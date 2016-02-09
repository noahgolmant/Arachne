package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.arachne.LocalRunner;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.document.TextDocumentStatistics;
import de.l3s.boilerpipe.extractors.ArticleSentencesExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.Date;
import java.util.Map;

/**
 * Created by noah on 2/8/16.
 */
public class ExtractionBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(CrawlerBolt.class);

    private Map<String,String> stormConfig;
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConfig = (Map<String,String>)map;
        this.outputCollector = outputCollector;
    }

    public static Fields boltFields = new Fields("url", "text", "date");

    private String getArticleText(String url, String html) throws Exception{
        TextDocument td = new BoilerpipeSAXInput(new InputSource(
                new StringReader(html))).getTextDocument();

        TextDocumentStatistics tdBefore = new TextDocumentStatistics(td, false);
        ArticleSentencesExtractor.INSTANCE.process(td);
        TextDocumentStatistics tdAfter = new TextDocumentStatistics(td, false);

        /* TODO get better article quality estimator check */
       /* if (SimpleEstimator.INSTANCE.isLowQuality(tdBefore, tdAfter)) {
            logger.info("not a good article: {}", url);
            return null;
        } else {
            logger.info("extracted text for {}: {}", url, td.getContent());
            return td.getContent();
        }*/
        logger.info("extracted text for {}: {}", url, td.getContent());
        return td.getContent();
    }

    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        String html = tuple.getStringByField("html");
        Date date = (Date)tuple.getValueByField("date");

        if (html == null || html.isEmpty()) {
            logger.error("No content for {}", url);
            outputCollector.ack(tuple);
            return;
        }

        try {
            String articleText = getArticleText(url, html);
            if (articleText != null)
                outputCollector.emit(LocalRunner.EXTRACTION_STREAM, new Values(url, articleText, date));
            outputCollector.ack(tuple);
        } catch (Exception e) {
            outputCollector.fail(tuple);
            logger.error("error extracting text from {}: {}", url, e);
            outputCollector.reportError(e);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(LocalRunner.EXTRACTION_STREAM, boltFields);
    }
}
