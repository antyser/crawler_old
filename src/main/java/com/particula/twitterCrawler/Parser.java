package com.particula.twitterCrawler;

import com.google.gson.*;
import com.ning.http.client.Response;
import com.particula.service.ExtendUrlService;
import com.particula.utils.KafkaFactory;
import com.particula.utils.Utils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by junliu on 6/1/15.
 */
public class Parser {
    public static final Logger LOGGER = LoggerFactory.getLogger(Parser.class);
    Producer<String, String> producer;
    private static Map<String, Future<Response>> futureResponseMap;
    int counterConsume = 0, counterProduce = 0;
    Properties prop;
    JsonParser parser = new JsonParser();
    Gson gson = new GsonBuilder().create();

    public Parser(String configDir) {
        this.futureResponseMap = new HashMap<>();
        Path appConfigPath = Paths.get(configDir, "app.properties");
        this.prop = Utils.loadProperties(appConfigPath);
        Path producerPath = Paths.get(configDir, "producer.properties");
        Path consumerPath = Paths.get(configDir, "consumer.properties");
        producer = KafkaFactory.createProducer(producerPath);

        consume(KafkaFactory.createConsumerStream(consumerPath,
                prop.getProperty("kafka.pages"),
                prop.getProperty("kafka.consume_group")));
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
        for (JsonObject tweetMeta : tweetsMetaList) {
            JsonArray tinyUrls = tweetMeta.getAsJsonArray("tiny_urls");
            for (JsonElement tinyUrlElem : tinyUrls) {
                String tinyUrl = tinyUrlElem.getAsString();
                LOGGER.info("tiny url: {}", tinyUrl);
                futureResponseMap.put(tinyUrl, ExtendUrlService.asyncExtendUrl(tinyUrl));
            }
        }

        for (JsonObject tweetMeta : tweetsMetaList) {
            JsonArray tinyUrls = tweetMeta.getAsJsonArray("tiny_urls");
            for (JsonElement tinyUrlElem : tinyUrls) {
                String tinyUrl = tinyUrlElem.getAsString();
                try {
                    String longUrl = futureResponseMap.get(tinyUrl).get().getUri().toUrl();
                    if (StringUtils.isEmpty(longUrl)) {
                        continue;
                    }
                    JsonObject outputObj = new JsonObject();
                    outputObj.addProperty("url", longUrl);
                    outputObj.add("tweet_meta", tweetMeta);
                    outputObj.addProperty("domain", getDomainName(longUrl));
                    if (data.has("ts_fetch")) {
                        tweetMeta.addProperty("ts_fetch", data.get("ts_fetch").getAsString());
                    }
                    produce(outputObj, prop.getProperty("kafka.links"));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

        }
        futureResponseMap.clear();
    }

    private List<JsonObject> parseTwitter(String htmlContent) {
        List<JsonObject> result = new ArrayList<>();
        Document doc = Jsoup.parse(htmlContent);
        Elements elements = doc.select("li[id^=stream-item-tweet-]");
        for (Element e : elements) {
            String tweetId = e.attr("data-item-id");
            String retweet = e.select("span[class^=ProfileTweet-action--retweet] > span.ProfileTweet-actionCount").attr("data-tweet-stat-count");
            String favorite = e.select("span[class^=ProfileTweet-action--favorite] > span.ProfileTweet-actionCount").attr("data-tweet-stat-count");
            String pubdate = e.select("span[data-time]").attr("data-time");
            String text = e.select("div.content > p").text();
            Elements tinyUrlElemList = e.select("div.content > p > a.twitter-timeline-link > span.js-display-url");
            JsonArray tinyUrls = new JsonArray();
            for (Element elem : tinyUrlElemList) {
                tinyUrls.add(new JsonPrimitive(elem.text()));
            }
            JsonObject object = new JsonObject();
            object.addProperty("tweet_id", tweetId);
            object.addProperty("retweet", retweet);
            object.addProperty("favorite", favorite);
            object.addProperty("pubdate", pubdate);
            object.addProperty("text", text);
            object.add("tiny_urls", tinyUrls);
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
        Parser p = new Parser("src/main/resources");
    }
}
