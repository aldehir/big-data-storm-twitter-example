package edu.utdallas.cs.bigdataproject.bolt;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import edu.utdallas.cs.bigdataproject.util.Stopwords;

public class ExtractTweetBolt implements IRichBolt {
    OutputCollector collector;

    Pattern splitPattern;
    Pattern removePattern;

    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        splitPattern = Pattern.compile("\\s+");
        removePattern = Pattern.compile("^(@|#|https?://)[^ ]+$");
    }

    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValue(0);
        String text = status.getText();

        // Split by space
        String[] split = splitPattern.split(text);

        // Construct new string
        StringBuilder process = new StringBuilder();
        for (String s : split) {
            if (removePattern.matcher(s).matches()) continue;
            if (Stopwords.isStopword(s)) continue;
            process.append(s);
            process.append(' ');
        }
        String result = process.toString().trim();
        
        collector.emit(tuple, new Values(result));
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-text"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
