package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.arachne.LocalRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by noah on 2/14/16.
 */
public class TokenizerBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(TokenizerBolt.class);

    private Map<String,String> stormConfig;
    private OutputCollector outputCollector;

    private Analyzer analyzer;
    private EnglishStemmer stemmer;

    public static final Fields boltFields = new Fields("url", "date", "text", "tokens");

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConfig = (Map<String,String>)map;
        this.outputCollector = outputCollector;

        this.analyzer = new EnglishAnalyzer();
        this.stemmer  = new EnglishStemmer();
    }

    private List<String> tokenizeString(String str) throws IOException {
        List<String> result = new ArrayList<String>();

        TokenStream stream = analyzer.tokenStream(null, new StringReader(str));
        stream.reset();
        while (stream.incrementToken()) {
            result.add(stream.getAttribute(CharTermAttribute.class).toString());
        }

        stream.end();
        stream.close();
        return result;
    }

    private List<String> stemTokens(List<String> tokens) {
        List<String> stemmed = new ArrayList<String>();

        for(String token : tokens) {
            stemmer.setCurrent(token);
            if (stemmer.stem())
                stemmed.add(stemmer.getCurrent());
            else
                stemmed.add(token);
        }
        return stemmed;
    }

    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        String text = tuple.getStringByField("text");
        Date date = (Date)tuple.getValueByField("date");

        /* tokenize text, filters stopwords */
        List<String> tokenizedText;
        try {
            tokenizedText = tokenizeString(text);
        } catch (IOException e) {
            logger.error("{} while tokenizing text for {}: {}", e.getMessage(), url, e.getStackTrace());
            outputCollector.ack(tuple);
            return;
        }

        if (tokenizedText == null) {
            logger.warn("No tokenized text available for {}", url);
            outputCollector.ack(tuple);
            return;
        }

        /* stem each token */
        tokenizedText = stemTokens(tokenizedText);

        /* emit tokens */
        /*StringBuilder tokenStr = new StringBuilder();
        for (String s : tokenizedText) {
            tokenStr.append(s + " -- ");
        }
        logger.info("TOKENIZED TEXT: {}", tokenStr.toString());*/

        outputCollector.emit(LocalRunner.TOKENIZER_STREAM, new Values(url, date, text, tokenizedText));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(LocalRunner.TOKENIZER_STREAM, boltFields);
    }
}
