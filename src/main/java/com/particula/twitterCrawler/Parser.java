package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.particula.service.ExtendUrlService;
import com.particula.utils.KafkaFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by junliu on 6/1/15.
 */
public class Parser {
    public static final Logger LOGGER = LoggerFactory.getLogger(Parser.class);
    Producer<String, String> producer;
    int counterConsume = 0, counterProduce = 0;
    Properties prop;
    JsonParser parser = new JsonParser();
    Gson gson = new GsonBuilder().create();

    public Parser(Properties prop) {
        producer = KafkaFactory.createProducer();
        this.prop = prop;
    }

    public void consume(KafkaStream<byte[], byte[]> stream) {
        LOGGER.info("fetcher start");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            LOGGER.info("parser consume : {}", counterConsume++);
            JsonObject data = (JsonObject) parser.parse(msg);
            process(data);
        }
    }

    private List<String> discoverUrls(String content) {
        Document doc = Jsoup.parse(content);
        Elements elements = doc.select("div.content > p > a.twitter-timeline-link > span.js-display-url");
        List<String> result = new ArrayList<>();
        for (org.jsoup.nodes.Element e : elements) {
            result.add(e.text());
        }
        return result;
    }

    public static String getDomainName(String url) {
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return "";
        }
        if (uri == null) return "";
        String domain = uri.getHost();
        if (domain == null) return "";
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    public void process(JsonObject data) {
        String htmlContent = data.get("data").getAsString();
        if (htmlContent == null) return;
        List<String> urls = discoverUrls(htmlContent);
        List<String> expendedUrl = ExtendUrlService.extendUrls(urls);
        for (String url : expendedUrl) {
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
        KeyedMessage<String, String> message = new KeyedMessage<>(topic, String.valueOf(counterProduce), msg);
        LOGGER.info("fetcher produce : {} {}", counterProduce++, msg);
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
            LOGGER.info("consume topic: groupid {}: {}", consumingTopic, groupId);
            p.consume(KafkaFactory.createConsumerStream(consumingTopic, groupId));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
