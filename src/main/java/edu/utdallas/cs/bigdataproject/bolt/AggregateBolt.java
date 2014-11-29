package edu.utdallas.cs.bigdataproject.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import edu.utdallas.cs.bigdataproject.storage.HashtagCounter;
import edu.utdallas.cs.bigdataproject.storage.RedisHashtagCounter;

public class AggregateBolt implements IRichBolt {
    OutputCollector collector;

    LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();

    int batchSize = 100;
    int batchIntervalInSec = 45;
    long lastBatchProcessTimeSeconds = 0;

    // CassandraClient client;
    HashtagCounter hashtagCounter;

    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        /*String cassandraNode = (String)conf.get("cassandra-node");
        client = new CassandraClient();
        client.connect(cassandraNode);

        hashtagCounter = new CassandraHashtagCounter(client);*/

        hashtagCounter = new RedisHashtagCounter(
            (String)conf.get("redis-server")
        );
    }

    public boolean isTickTuple(Tuple tuple) {
        return (
            tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
            tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)
        );
    }

    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            long elapsedTime = (System.currentTimeMillis() / 1000)
                               - lastBatchProcessTimeSeconds;

            if (elapsedTime >= batchIntervalInSec) {
                finishBatch();
            }
        } else {
            this.queue.add(tuple);
            int queueSize = this.queue.size();

            if (queueSize >= batchSize) {
                finishBatch();
            }
        }
    }

    public void finishBatch() {
        lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;

        List<Tuple> tuples = new ArrayList<Tuple>();
        queue.drainTo(tuples);

        // Construct a map where we will store the hashtags and a count
        Map<String, Long> counts = new HashMap<String, Long>();

        // Sum up all of the hashtag counts
        for (Tuple tuple : tuples) {
            String hashtag = tuple.getString(0);

            Long previous = new Long(0);
            if (counts.containsKey(hashtag)) {
                previous = counts.get(hashtag);
            }

            counts.put(hashtag, new Long(previous.intValue() + 1));

            // Acknowledge tuple, this may need to be moved at the end of the
            // function? Might not matter since the tuples we emit below are
            // unanchored.
            collector.ack(tuple);
        }

        // Store counts
        hashtagCounter.increment(counts);

        // Emit new tuples
        /*for (Map.Entry<String, Integer> count : counts.entrySet()) {
            collector.emit(new Values(count.getKey(), count.getValue()));
        }*/
    }

    public void cleanup() {
        // client.close();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
