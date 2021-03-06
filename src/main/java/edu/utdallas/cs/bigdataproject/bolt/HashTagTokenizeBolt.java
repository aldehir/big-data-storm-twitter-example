package edu.utdallas.cs.bigdataproject.bolt;

import java.util.Map;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashTagTokenizeBolt implements IRichBolt {
    OutputCollector collector;

    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValue(0);
        collector.emit(tuple, new Values(status.getText()));
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
