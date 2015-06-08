package com.particula.utils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class KafkaFactory {
    public static Producer createProducer(Path configPath) {
        Properties props = Utils.loadProperties(configPath);
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        return producer;
    }

    public static KafkaStream<byte[], byte[]> createConsumerStream(Path configPath, String topic, String groupId) {
        Properties consumerProperties = Utils.loadProperties(configPath);
        consumerProperties.put("group.id", groupId);
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        KafkaStream stream = consumerConnector.createMessageStreams(topicCountMap).get(topic).get(0);
        return stream;
    }
}