package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.particula.utils.KafkaFactory;
import com.particula.utils.Utils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Fetcher {
    public static final Logger LOGGER = LoggerFactory.getLogger(Fetcher.class);
    Producer<String, String> producer;
    int counterIn = 0, counterOut = 0;
    Properties prop;
    JsonParser parser = new JsonParser();
    Gson gson = new GsonBuilder().create();

    public Fetcher(String configDir) {
        Path appConfigPath = Paths.get(configDir, "app.properties");
        this.prop = Utils.loadProperties(appConfigPath);
        Path producerPath = Paths.get(configDir, "producer.properties");
        Path consumerPath = Paths.get(configDir, "consumer.properties");
        producer = KafkaFactory.createProducer(producerPath);
        consume(KafkaFactory.createConsumerStream(consumerPath,
                prop.getProperty("kafka.seeds"),
                prop.getProperty("kafka.consume_group")));
    }

    private String getHtml(String url) {
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = con.getResponseCode();
            if (responseCode != 200) {
                return null;
            }
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void consume(KafkaStream<byte[], byte[]> stream) {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            LOGGER.info("fetcher consume: {}", counterIn++);
            JsonObject data = (JsonObject) parser.parse(msg);
            process(data);
        }
    }

    public void process(JsonObject data) {
        String url = data.get("url").getAsString();
        String htmlContent = getHtml(url);
        if (htmlContent == null) return;
        data.addProperty("content", htmlContent);
        //add original url
        data.addProperty("ts_fetch", String.valueOf(new Date().getTime() / 1000));
        Map<String, String> outputData = new HashMap<>();
        outputData.put("data", htmlContent);
        produce(outputData, prop.getProperty("kafka.pages"));
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        LOGGER.info("fetcher produce: {}", counterOut++);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, String.valueOf(counterOut), msg);
        producer.send(message);
    }

    public static void main(String[] args) {
        Fetcher f = new Fetcher("src/main/resources");
    }
}
