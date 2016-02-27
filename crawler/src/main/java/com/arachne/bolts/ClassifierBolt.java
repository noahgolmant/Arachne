package com.arachne.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by noah on 2/16/16.
 */
public class ClassifierBolt extends BaseRichBolt {

    Map<String, String> stormConfig;
    OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConfig = (Map<String,String>)map;
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
