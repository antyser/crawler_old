package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.particula.utils.KafkaFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by junliu on 6/1/15.
 */
public class Seeder {
    private static final String SEEDS_QUEUE = "java.test.seeds";
    Producer<String, String> producer;
    Gson gson = new GsonBuilder().create();

    public Seeder() {
        producer = KafkaFactory.createProducer();
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        System.out.println("input: " + msg);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, msg);
        producer.send(message);
    }

    public void process() {
        try (BufferedReader br = new BufferedReader(new FileReader("top20_account.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, String> data = new HashMap<>();
                data.put("url", line);
                produce(data, SEEDS_QUEUE);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Seeder s = new Seeder();
        s.process();
    }
}
