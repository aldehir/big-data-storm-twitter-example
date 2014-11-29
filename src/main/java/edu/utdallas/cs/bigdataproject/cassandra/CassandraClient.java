package edu.utdallas.cs.bigdataproject.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraClient {
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder()
            .addContactPoint(node)
            .build();

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }

        session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        cluster.close();
    }
}
