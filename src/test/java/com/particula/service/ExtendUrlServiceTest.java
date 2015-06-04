package com.particula.service;

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by junliu on 6/4/15.
 */
public class ExtendUrlServiceTest {

    @Test
    public void testExtendUrl() throws Exception {

    }

    @Test
    public void testExtendUrls() throws Exception {
        List<String> urls = new ArrayList<>();
        urls.add("cnn.it/1Imbd6d");
        urls.add("http://www.snappytv.com/tc/626997");
        Set<String> expectedSet = new HashSet<>();
        expectedSet.add("http://www.cnn.com/2015/06/04/us/chandra-levy-trial/index.html");
        expectedSet.add("http://www.snappytv.com/tc/626997");
        List<String> expendUrls = ExtendUrlService.extendUrls(urls);
        Assert.assertNotNull(expendUrls);
        Assert.assertEquals(expendUrls.size(), 2);
        Set<String> set = new HashSet<>(expendUrls);
        Assert.assertEquals(set, expectedSet);
    }
}