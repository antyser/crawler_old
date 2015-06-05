package com.particula.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by junliu on 6/4/15.
 */
public class ExtendUrlConsumer implements Runnable {
    private BlockingQueue<List<String>> outQueue;
    private IUrlExtender extender;
    private List<String> inputUrls;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtendUrlConsumer.class);

    public ExtendUrlConsumer(List<String> inputUrls, BlockingQueue<List<String>> outQueue, IUrlExtender extender) {
        this.inputUrls = inputUrls;
        this.outQueue = outQueue;
        this.extender = extender;
    }

    @Override
    public void run() {
        try {
            List<String> extendedUrls = extender.extendUrls(inputUrls);
            LOGGER.info("{} extend urls: {}", extender.getClass().getSimpleName(), extendedUrls);
            outQueue.put(extendedUrls);
        } catch (InterruptedException e) {
            LOGGER.error("Exception in extend url service ", e);
        }
    }
}
