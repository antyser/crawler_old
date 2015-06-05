package com.particula.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by junliu on 6/4/15.
 */
public class LongUrlExtender implements IUrlExtender {
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

    @Override
    public List<String> extendUrls(List<String> shortenedUrls) {
        List<String> result = new ArrayList<>();
        for (String tinyUrls : shortenedUrls) {
            String url = extendUrl(tinyUrls);
            if (url != null) {
                result.add(url);
            }
        }
        return result;
    }
}
