package com.arachne.kafka;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Properties;

/**
 * Created by ravi on 2/24/16.
 */
public class KafkaURLProducer<K, V> {

    private static final String keyspace = "arachne";

    public static void main(String[] args) {
        Properties props = new Properties();
        Cluster cluster;
        Session session;
        ResultSet results;

        cluster = Cluster
                .builder()
                .addContactPoint("localhost")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();
        session = cluster.connect(keyspace);

        props.put("metadata.broker.list", "broker:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.arachne.kafka.URLPartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);


        PreparedStatement statement = session.prepare(

                "INSERT INTO urls" + "(url)"
                        + "VALUES (?);");

        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.bind("www.google.com"));
        session.execute(boundStatement.bind("www.yahoo.com"));

        Statement select = QueryBuilder.select().all().from(keyspace, "urls");
        results = session.execute(select);

        for (Row row : results) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("urls", row.getString("url"));

            producer.send(data);
        }
        session.close();
        cluster.close();
        producer.close();
    }
}
