package edu.utdallas.cs.bigdataproject;

import twitter4j.TwitterStreamFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import edu.utdallas.cs.bigdataproject.bolt.AggregateBolt;
import edu.utdallas.cs.bigdataproject.bolt.HashTagTokenizeBolt;
import edu.utdallas.cs.bigdataproject.cassandra.CassandraClient;
import edu.utdallas.cs.bigdataproject.spout.TwitterStreamSpout;
import edu.utdallas.cs.bigdataproject.storage.CassandraHashtagCounter;
import edu.utdallas.cs.bigdataproject.storage.HashtagCounter;

public class TwitterTopology {
    public static void main(String[] args) {
        TwitterStreamFactory factory = new TwitterStreamFactory();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitterStreamSpout(factory), 1);
        builder.setBolt("hashtags", new HashTagTokenizeBolt(), 10)
               .shuffleGrouping("tweets");
        builder.setBolt("counts", new AggregateBolt(), 5)
               .fieldsGrouping("hashtags", new Fields("hashtag"));

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(5);
        config.put("cassandra-node", "localhost");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Test", config, builder.createTopology());
        Utils.sleep(60000);
        cluster.killTopology("Test");
        cluster.shutdown();
    }
}
