package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.particula.utils.KafkaFactory;
import com.particula.utils.Utils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Seeder {
    private static final String INITIAL_URL = "https://twitter.com/";
    public static final Logger LOGGER = LoggerFactory.getLogger(Seeder.class);
    Producer<String, String> producer;
    Properties prop;
    Gson gson = new GsonBuilder().create();
    int counter = 0;

    public Seeder(String configDir) {
        Path appConfigPath = Paths.get(configDir, "app.properties");
        this.prop = Utils.loadProperties(appConfigPath);
        Path producerPath = Paths.get(configDir, "producer.properties");
        producer = KafkaFactory.createProducer(producerPath);
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, String.valueOf(counter), msg);
        LOGGER.info("output: {}  {}", msg, counter++);
        producer.send(message);
    }

    public void process() {
        try (BufferedReader br = new BufferedReader(new FileReader(prop.getProperty("seed_list")))) {
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, String> data = new HashMap<>();
                data.put("url", INITIAL_URL + line);
                data.put("label", "twitter");
                data.put("ts_task", String.valueOf(new java.util.Date().getTime() / 1000));
                produce(data, prop.getProperty("kafka.seeds"));
            }
            producer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Seeder s = new Seeder("src/main/resources");
        s.process();
    }
}