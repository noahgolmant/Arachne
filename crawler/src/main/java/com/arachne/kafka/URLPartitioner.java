package com.arachne.kafka;

/**
 * Created by ravi on 3/5/16.
 */
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class URLPartitioner implements Partitioner {
    public URLPartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        return partition;
    }
}