package edu.utdallas.cs.bigdataproject.storage;

import java.util.Map;

public interface HashtagCounter {
    public void increment(String hashtag, long value);
    public void increment(Map<String, Long> values);
}
