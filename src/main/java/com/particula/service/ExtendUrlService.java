package com.particula.service;

import com.google.common.base.Joiner;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by junliu on 6/3/15.
 */
public class ExtendUrlService {
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

    public static List<String> extendUrls(List<String> shortenedUrls) {
        List<String> result = new ArrayList<>();
        try {
            if (shortenedUrls.size() == 0) return shortenedUrls;
            String queryUrl = "http://urlex.org/json/";
            String params = Joiner.on("***").skipNulls().join(shortenedUrls);
            URL obj = new URL(queryUrl + params);
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

            for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
                if (!entry.getValue().isJsonPrimitive()) {
                    continue;
                }
                if (entry.getValue().getAsJsonPrimitive().isString()) {
                    result.add(entry.getValue().getAsString());
                } else {
                    result.add(entry.getKey());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return result;
        }
        return result;
    }
}
