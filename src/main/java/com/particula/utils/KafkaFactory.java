package com.particula.utils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class KafkaFactory {
    public static Properties loadConfig(){
     Properties prop = new Properties();
        String path = new File("src/main/resources/config.properties")
                .getAbsolutePath();
        try {
            prop.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    public static Producer createProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", loadConfig().get("kafka.address"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        return producer;
    }


    public static KafkaStream<byte[], byte[]> createConsumerStream(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", loadConfig().get("zookeeper"));
        properties.put("group.id", groupId);
        properties.put("fetch.message.max.bytes", String.valueOf(20 * 1024 * 1024));
        //turn this on because some files are stuck in kafka queue. There will be performance penalty
        properties.put("compression.codec", "gzip");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        KafkaStream stream = consumerConnector.createMessageStreams(topicCountMap).get(topic).get(0);
        return stream;
    }
}
