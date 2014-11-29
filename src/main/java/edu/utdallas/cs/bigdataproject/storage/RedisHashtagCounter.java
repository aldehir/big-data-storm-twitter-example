package edu.utdallas.cs.bigdataproject.storage;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisHashtagCounter implements HashtagCounter {
    private String server;
    private JedisPool pool;

    public RedisHashtagCounter(String server) {
        this.server = server;
        pool = new JedisPool(new JedisPoolConfig(), server);
    }

    public void increment(String hashtag, long value) {
        try (Jedis jedis = pool.getResource()) {
            jedis.zincrby("hashtagcounts", value, hashtag);
        }
    }

    public void increment(Map<String, Long> values) {
        try (Jedis jedis = pool.getResource()) {
            for (Map.Entry<String, Long> entry : values.entrySet()) {
                jedis.zincrby("hashtagcounts",
                              entry.getValue().doubleValue(),
                              entry.getKey());
            }
        }
    }
}
