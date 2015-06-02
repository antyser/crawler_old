package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.particula.utils.KafkaFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by junliu on 6/1/15.
 */
public class Fetcher {
    private static final String SEEDS_QUEUE = "java.test.seeds";
    private static final String PAGES_QUEUE = "java.test.pages";
    Producer<String, String> producer;
    Gson gson = new GsonBuilder().create();
    Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();

    public Fetcher() {
        producer = KafkaFactory.createProducer();
    }

    private String getHtml(String url) {
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = con.getResponseCode();
            System.out.println("Sending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);
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
            Map<String, String> data = gson.fromJson(msg, mapType);
            process(data);
        }
    }

    public void process(Map<String, String> data) {
        String htmlContent = data.get("url");
        if (htmlContent == null) return;
        Map<String, String> outputData = new HashMap<String, String>();
        outputData.put("data", htmlContent);
        outputData.put("seed", data.get("url"));
        outputData.put("dl_ts", String.valueOf(new java.util.Date().getTime()));
        produce(outputData, PAGES_QUEUE);
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        System.out.println("input: " + msg);
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, msg);
        producer.send(message);
    }

    public static void main(String[] args) {
        Fetcher f = new Fetcher();
        f.consume(KafkaFactory.createConsumerStream(SEEDS_QUEUE));
    }
}
