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

import edu.utdallas.cs.bigdataproject.util.Stopwords;

import org.tartarus.snowball.ext.englishStemmer;
import org.tartarus.snowball.SnowballStemmer;

public class PorterStemmerBolt implements IRichBolt {
    OutputCollector collector;
    SnowballStemmer stemmer;

    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        stemmer = (SnowballStemmer)(new englishStemmer());
    }

    public void execute(Tuple tuple) {
        String text = (String)tuple.getValue(0);
        StringBuilder buffer = new StringBuilder();

        // Pass through the stemmer
        String[] split = text.split("\\s+");
        for (String s : split) {
            stemmer.setCurrent(s);
            stemmer.stem();
            buffer.append(stemmer.getCurrent());
            buffer.append(' ');
        }

        collector.emit(tuple, new Values(buffer.toString().trim()));
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
