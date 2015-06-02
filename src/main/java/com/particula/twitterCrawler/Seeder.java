package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.particula.utils.KafkaFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Seeder {
    private static final String INITIAL_URL = "https://twitter.com/";
    Producer<String, String> producer;
    Properties prop;
    Gson gson = new GsonBuilder().create();
    int counter = 0;

    public Seeder(Properties prop) {
        this.prop = prop;
        producer = KafkaFactory.createProducer();
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        System.out.println("input: " + msg);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, String.valueOf(counter), msg);
        counter++;
        producer.send(message);
    }

    public void process() {
        try (BufferedReader br = new BufferedReader(new FileReader("top20_account.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, String> data = new HashMap<>();
                data.put("url", INITIAL_URL + line);
                produce(data, prop.getProperty("kafka.seeds"));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("resources/config.properties"));
            Seeder s = new Seeder(prop);
            s.process();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
