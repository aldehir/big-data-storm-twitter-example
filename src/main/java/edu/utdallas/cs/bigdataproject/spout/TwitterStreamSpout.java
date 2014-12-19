package edu.utdallas.cs.bigdataproject.spout;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Twitter Stream Spout.
 * Storm Spout for Twitter's public Streaming API.
 */
public class TwitterStreamSpout extends BaseRichSpout {
    SpoutOutputCollector collector;

    LinkedBlockingQueue<Status> queue = null;

    TwitterStreamFactory twitterStreamFactory;
    TwitterStream twitterStream;

    public TwitterStreamSpout(TwitterStreamFactory streamFactory) {
        twitterStreamFactory = streamFactory;
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) { }

            @Override
            public void onTrackLimitationNotice(int i) { }

            @Override
            public void onScrubGeo(long l, long l1) { }

            @Override
            public void onStallWarning(StallWarning w) { }

            @Override
            public void onException(Exception e) { }
        };

        twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if (status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }


    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.setMaxTaskParallelism(1);
        return config;
    }

    @Override
    public void ack(Object id) { }

    @Override
    public void fail(Object id) { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
