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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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

    private NodeList getTags(String content) {
        DocumentBuilderFactory builderFactory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = null;
        try {
            builder = builderFactory.newDocumentBuilder();
            Document document = builder.parse(new ByteArrayInputStream(content.getBytes()));
            XPath xPath = XPathFactory.newInstance().newXPath();
            String expression = "//*[@class='content']/p//a[@class='twitter-timeline-link']";
            NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(document, XPathConstants.NODESET);
            return nodeList;
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
        return null;
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
        NodeList nodeList = getTags(htmlContent);
        System.out.println("nodelist len: " + nodeList.getLength());
        if (nodeList == null) return;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
                Element el = (Element) nodeList.item(i);
                String shortenedUrl = el.getAttribute("href");
                String url = extendUrl(shortenedUrl);
                System.out.println("expended url: " + url);
                JsonObject output = new JsonObject();
                output.addProperty("url", url);
                output.addProperty("domain", getDomainName(url));
                output.addProperty("score", 1);
                output.addProperty("dl_ts", data.get("dl_ts").getAsString());
                produce(output, prop.getProperty("kafka.links"));
            }
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
            p.consume(KafkaFactory.createConsumerStream(prop.getProperty("kafka.pages")));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
