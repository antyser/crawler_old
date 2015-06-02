package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.particula.utils.KafkaFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Fetcher {
    Producer<String, String> producer;
    int counter = 0;
    Gson gson = new GsonBuilder().create();
    Properties prop;
    Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();

    public Fetcher(Properties prop) {
        producer = KafkaFactory.createProducer();
        this.prop = prop;
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
        System.out.println("fetcher starts");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            System.out.println("input: " + msg);
            Map<String, String> data = gson.fromJson(msg, mapType);
            process(data);
        }
    }

    public void process(Map<String, String> data) {
        String url = data.get("url");
        String htmlContent = getHtml(url);
        if (htmlContent == null) return;
        Map<String, String> outputData = new HashMap<>();
        outputData.put("data", htmlContent);
        outputData.put("seed", data.get("url"));
        outputData.put("dl_ts", String.valueOf(new java.util.Date().getTime() / 1000));
        produce(outputData, prop.getProperty("kafka.pages"));
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        System.out.println("output: " + msg);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, String.valueOf(counter), msg);
        counter++;
        producer.send(message);
    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        try {
            String path = new File("src/main/resources/config.properties")
                    .getAbsolutePath();
            prop.load(new FileInputStream(path));
            Fetcher f = new Fetcher(prop);
            f.consume(KafkaFactory.createConsumerStream(prop.getProperty("kafka.seeds")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
