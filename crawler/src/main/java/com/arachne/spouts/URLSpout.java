package com.arachne.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.arachne.DBConnection;
import com.arachne.bolts.FilterBolt;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class URLSpout extends BaseRichSpout {

    static ArrayList<String> testURLs = new ArrayList<String>();

    public static void getURLS() {
        Statement statement = QueryBuilder.select().column("url").from("urls");

        ResultSet results = DBConnection.getSession().execute(statement);
        for(Row r : results) {
            testURLs.add(r.getString(0));
        }


    }
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
        outputCollector.emit(new Values(testURLs.get(i % testURLs.size()), now), testURLs.get(i % testURLs.size()));
        i++;
    }
}