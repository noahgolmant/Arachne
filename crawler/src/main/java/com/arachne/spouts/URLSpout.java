package com.arachne.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.arachne.bolts.FilterBolt;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class URLSpout extends BaseRichSpout {

    String testURLs[] = {"http://www.cnn.com/2016/02/08/politics/michael-bloomberg-2016-election/index.html",
            "http://www.nytimes.com/2016/02/09/us/politics/new-hampshire-voters-hear-candidates-final-appeals-before-primary.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=a-lede-package-region&region=top-news&WT.nav=top-news&_r=0", "http://nytimes.com"};
    static int i = 0;

    SpoutOutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(FilterBolt.boltFields);
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;

    }

    public void nextTuple() {
        Utils.sleep(500);
        Date now = Calendar.getInstance().getTime();
        outputCollector.emit(new Values(testURLs[i % testURLs.length], now), testURLs[i % testURLs.length]);
        i++;
    }
}