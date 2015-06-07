package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ning.http.client.Response;
import com.particula.service.ExtendUrlService;
import com.particula.utils.KafkaFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
        try {
            return new URL(url).getHost();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return "";
    }

    public void process(JsonObject data) {
        String htmlContent = data.get("data").getAsString();
        if (htmlContent == null) return;
        List<JsonObject> tweetsMetaList = parseTwitter(htmlContent);
        Map<String, Future<Response>> urlExtendFuture = new HashMap<>();
        for (JsonObject tweetMeta : tweetsMetaList) {
            String tinyUrl = tweetMeta.getAsJsonPrimitive("tiny_url").getAsString();
            if (tinyUrl.length() == 0){
                continue;
            }
            LOGGER.info("tiny url: {}", tinyUrl);
            urlExtendFuture.put(tinyUrl, ExtendUrlService.asyncExtendUrl(tinyUrl));
        }

        for (JsonObject tweetMeta : tweetsMetaList) {
            String tinyUrl = tweetMeta.getAsJsonPrimitive("tiny_url").getAsString();
            if (tinyUrl.length() == 0){
                continue;
            }
            String longUrl = "";
            try {
                longUrl = urlExtendFuture.get(tinyUrl).get().getUri().toUrl();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            tweetMeta.addProperty("url", longUrl);
            if (longUrl.length() == 0) continue;
            tweetMeta.addProperty("domain", getDomainName(longUrl));
            tweetMeta.addProperty("score", 1);
            tweetMeta.addProperty("dl_ts", data.get("dl_ts").getAsString());
            produce(tweetMeta, prop.getProperty("kafka.links"));
        }
    }

    private List<JsonObject> parseTwitter(String htmlContent) {
        List<JsonObject> result = new ArrayList<>();
        Document doc = Jsoup.parse(htmlContent);
        Elements elements = doc.select("li[id^=stream-item-tweet-]");
        for (Element e : elements) {
            String tinyurl = e.select("div.content > p > a.twitter-timeline-link > span.js-display-url").text();
            String tweetId = e.attr("data-item-id");
            String retweet = e.select("span[class^=ProfileTweet-action--retweet] > span.ProfileTweet-actionCount").attr("data-tweet-stat-count");
            String favorite = e.select("span[class^=ProfileTweet-action--favorite] > span.ProfileTweet-actionCount").attr("data-tweet-stat-count");
            String pubdate = e.select("span[data-time]").attr("data-time");
            JsonObject object = new JsonObject();
            object.addProperty("tiny_url", tinyurl);
            object.addProperty("tweet_id", tweetId);
            object.addProperty("retweet", retweet);
            object.addProperty("favorite", favorite);
            object.addProperty("pubdate", pubdate);
            result.add(object);
        }
        return result;
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
