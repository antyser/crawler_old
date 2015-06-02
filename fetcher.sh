#!/bin/bash
while true; do
	mvn exec:java -Dexec.mainClass="com.particula.twitterCrawler.Fetcher" &
done
