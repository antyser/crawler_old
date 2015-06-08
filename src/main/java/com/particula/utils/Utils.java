package com.particula.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Created by junliu on 6/8/15.
 */
public class Utils {
    public static Properties loadProperties(Path path) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(path.toFile()));
            return prop;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }
}
