package com.particula.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by junliu on 6/3/15.
 */
public class ExtendUrlService {
    public static Future<Response> asyncExtendUrl(String shortenedUrl) {
        try {
            AsyncHttpClient client = new AsyncHttpClient();
            Future<Response> future = client.prepareGet("http://" + shortenedUrl).setFollowRedirects(true).addHeader("User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko)" +
                            " Chrome/43.0.2357.81 Safari/537.36").execute();
            return future;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String extendUrl(String shortenedUrl) {
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

    public static List<String> extendUrls(List<String> shortenUrls) {
        List<Future<Response>> futures = new ArrayList<>();
        for (String shortenUrl: shortenUrls) {
            futures.add(asyncExtendUrl(shortenUrl));
        }
        List<String> urls = new ArrayList<>();
        for (Future<Response> future : futures) {
            try {
                urls.add(future.get().getUri().toUrl());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return urls;
    }
}
