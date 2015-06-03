package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.particula.utils.KafkaFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Parser {
    Producer<String, String> producer;
    int counter = 0;
    Properties prop;
    JsonParser parser = new JsonParser();
    Gson gson = new GsonBuilder().create();

    public Parser(Properties prop) {
        producer = KafkaFactory.createProducer();
        this.prop = prop;
    }

    public void consume(KafkaStream<byte[], byte[]> stream) {
        System.out.println("parser starts");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            //System.out.println("input: " + msg);
            JsonObject data = (JsonObject) parser.parse(msg);
            process(data);
        }
    }

    private List<String> discoverUrls(String content) {
        Document doc = null;
        doc = Jsoup.parse(content);
        Elements elements = doc.select("div.content > p > a.twitter-timeline-link");
        List<String> result = new ArrayList<>();
        for (org.jsoup.nodes.Element e : elements) {
            result.add(e.attr("href"));
        }
        return result;
    }

    //TODO(Isaac): make it a service
    public String extendUrl(String shortenedUrl) {
        try {
            String queryUrl = "http://api.longurl.org/v2/expand?format=json&url=";
            URL obj = new URL(queryUrl + shortenedUrl);
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
            JsonParser parser = new JsonParser();
            JsonObject object = (JsonObject) parser.parse(response.toString());
            return object.get("long-url").getAsString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getDomainName(String url) {
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        String domain = uri.getHost();
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    public void process(JsonObject data) {
        String htmlContent = data.get("data").getAsString();
        if (htmlContent == null) return;
        List<String> urls = discoverUrls(htmlContent);
        for (String shortenedUrl : urls) {
            String url = extendUrl(shortenedUrl);
            if (url == null) continue;
            JsonObject output = new JsonObject();
            output.addProperty("url", url);
            output.addProperty("domain", getDomainName(url));
            output.addProperty("score", 1);
            output.addProperty("dl_ts", data.get("dl_ts").getAsString());
            produce(output, prop.getProperty("kafka.links"));
        }
    }

    public void produce(JsonObject data, String topic) {
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
            Parser p = new Parser(prop);
            String consumingTopic = prop.getProperty("kafka.pages");
            String groupId = prop.getProperty("kafka.consume_group");
            System.out.println("consume topic: groupid " + consumingTopic + ": " + groupId);
            p.consume(KafkaFactory.createConsumerStream(consumingTopic, groupId));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
