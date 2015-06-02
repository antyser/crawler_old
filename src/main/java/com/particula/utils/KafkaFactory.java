package com.particula.utils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class KafkaFactory {
    public static Producer createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "172.31.10.154:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        return producer;
    }


    public static KafkaStream<byte[], byte[]> createConsumerStream(String topic) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "172.31.24.55:2181,172.31.45.93:2181,172.31.4.157:2181");
        properties.put("group.id", "fetcher0601");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        KafkaStream stream = consumerConnector.createMessageStreams(topicCountMap).get(topic).get(0);
        return stream;
    }
}
