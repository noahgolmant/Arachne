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

    String testURLs[] = {"http://www.google.com", "http://www.cnn.com", "http://nytimes.com"};
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