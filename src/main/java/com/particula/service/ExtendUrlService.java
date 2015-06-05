package com.particula.service;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by junliu on 6/3/15.
 */
public class ExtendUrlService {
    private BlockingQueue<List<String>> inQueue, outQueue;
    private IUrlExtender[] extenders = new IUrlExtender[4];
    private int counter = 0;

    public ExtendUrlService() {
        inQueue = new ArrayBlockingQueue(1024);
        outQueue = new ArrayBlockingQueue(1024);
        extenders[0] = new UrlexUrlExtender();
        extenders[1] = new LongUrlExtender();
        extenders[2] = new UrlexUrlExtender();
        extenders[3] = new LongUrlExtender();
        process();
    }

    public void consume(List<String> urlList) {
        try {
            inQueue.put(urlList);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void process() {
        while (true) {
            List<String> urlList = null;
            try {
                urlList = inQueue.take();
                ExtendUrlConsumer comsumer = new ExtendUrlConsumer(urlList, outQueue, extenders[counter % extenders.length]);
                new Thread(comsumer).start();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<String> produce() {
        try {
            return outQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
