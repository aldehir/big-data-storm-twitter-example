package edu.utdallas.cs.bigdataproject.storage;

import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import edu.utdallas.cs.bigdataproject.cassandra.CassandraClient;

public class CassandraHashtagCounter implements HashtagCounter {
    private CassandraClient client;

    public CassandraHashtagCounter(CassandraClient client) {
        this.client = client;
    }

    public void increment(String hashtag, long value) {
        Session session = client.getSession();

        PreparedStatement statement = session.prepare(
            "UPDATE twitterstream.hashtag_counts SET count = count + ? " +
            "WHERE hashtag = ?;");

        BoundStatement boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(
            new Long(value),
            hashtag));
    }

    public void increment(Map<String, Long> values) {
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            increment(entry.getKey(), entry.getValue().longValue());
        }
    }

    public static void main(String[] args) {
        CassandraClient client = new CassandraClient();
        client.connect("localhost");

        HashtagCounter counter = new CassandraHashtagCounter(client);
        counter.increment("test1", 2);
        counter.increment("test1", 3);
        counter.increment("test2", 1);

        client.close();
    }
}
