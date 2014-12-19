package edu.utdallas.cs.bigdataproject;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import twitter4j.TwitterStreamFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import edu.utdallas.cs.bigdataproject.bolt.HashTagTokenizeBolt;
import edu.utdallas.cs.bigdataproject.bolt.ExtractTweetBolt;
import edu.utdallas.cs.bigdataproject.bolt.PorterStemmerBolt;
import edu.utdallas.cs.bigdataproject.spout.TwitterStreamSpout;

public class TwitterTopology {

    static class MyFileNameFormat extends DefaultFileNameFormat {
        private long rotations = 0;

        public MyFileNameFormat withRotations(long rotations) {
            this.rotations = rotations;
            return this;
        }

        public String getPath(long rotation, long timestamp) {
            if ( this.rotations == 0 ) return super.getPath(rotation, timestamp);
            return super.getPath(rotation, timestamp) + "-" + (rotation / this.rotations);
        }
    }

    public static void main(String[] args) throws Exception {
        TwitterStreamFactory factory = new TwitterStreamFactory();

        if (args.length < 3) {
            System.err.println("Usage: TwitterTopology <remote/local> <hdfs-server> <output-dir>");
            System.exit(1);
        }

        boolean isLocal = args[0].equals("local");
        String hdfsServer = args[1];
        String path = args[2];

        FileNameFormat fileNameFormat = new MyFileNameFormat()
            .withRotations(3)
            .withPath(path);

        FileNameFormat stemmedFileNameFormat = new MyFileNameFormat()
            .withRotations(3)
            .withPath(path + "-stemmed");

        RecordFormat format = new DelimitedRecordFormat()
            .withFieldDelimiter("|");

        SyncPolicy syncPolicy = new CountSyncPolicy(50);

        // FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(5.0f, TimeUnit.MINUTES);

        HdfsBolt hdfsBolt = new HdfsBolt()
            .withFsUrl(hdfsServer)
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);

        HdfsBolt hdfsBoltStemmed = new HdfsBolt()
            .withFsUrl(hdfsServer)
            .withFileNameFormat(stemmedFileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets", new TwitterStreamSpout(factory), 1);
        builder.setBolt("tweet-text", new ExtractTweetBolt(), 10)
               .shuffleGrouping("tweets");
        builder.setBolt("stemming", new PorterStemmerBolt(), 10)
                .shuffleGrouping("tweet-text");

        builder.setBolt("store-plain", hdfsBolt, 1)
               .shuffleGrouping("tweet-text");
        builder.setBolt("store-stemmed", hdfsBoltStemmed, 1)
               .shuffleGrouping("stemming");

        // builder.setBolt("counts", new AggregateBolt(), 5)
        //        .fieldsGrouping("hashtags", new Fields("hashtag"));

        Config config = new Config();
        config.put("cassandra-node", "localhost");
        config.put("redis-server", "localhost");

        if (isLocal)
        {
            config.setDebug(true);
            config.setNumWorkers(5);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Test", config, builder.createTopology());
            Utils.sleep(60000*10);
            cluster.killTopology("Test");
            cluster.shutdown();
        }
        else
        {
            config.setNumWorkers(15);
            config.setMaxSpoutPending(5000);

            StormSubmitter.submitTopology("tweet-collector", config,
                    builder.createTopology());
        }
    }
}
