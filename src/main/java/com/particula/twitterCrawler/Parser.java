package com.particula.twitterCrawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by junliu on 6/1/15.
 */
public class Parser {
    private static final String PAGES_QUEUE = "java.test.pages";
    private static final String LINKS_QUEUE = "java.test.links";
    Producer<String, String> producer;
    Gson gson = new GsonBuilder().create();
    Type mapType = new TypeToken<Map<String, String>>() {
    }.getType();

    public Parser() {
        producer = KafkaFactory.createProducer();
    }

    public void consume(KafkaStream<byte[], byte[]> stream) {
        System.out.println("parser starts");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            Map<String, String> data = gson.fromJson(msg, mapType);
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
            String test = "//*[@class=";
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

    public void process(Map<String, String> data) {
        String htmlContent = data.get("url");
        if (htmlContent == null) return;
        NodeList nodeList = getTags(htmlContent);
        if (nodeList == null) return;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeType() == Node.ELEMENT_NODE) {
                Element el = (Element) nodeList.item(i);
                if (el.getNodeName().contains("staff")) {
                    String shortenedUrl = el.getAttribute("href");

                }
            }
        }
    }

    public void produce(Map<String, String> data, String topic) {
        String msg = gson.toJson(data);
        System.out.println("input: " + msg);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, msg);
        producer.send(message);
    }

    public static void main(String[] args) {
        Parser p = new Parser();
        p.consume(KafkaFactory.createConsumerStream(PAGES_QUEUE));
    }
}
