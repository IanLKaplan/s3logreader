/*
   This software is published under the Apache 2 software license.
 */

package com.topstonesoftware.s3logreader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Read a list of S3 log files can insert the lines read into a LinkedBlockingQueue.
 * </p>
 * <p>
 *     This class has a builder that is constructed via the Lombok @Builder annotation.
 *     All arguments must be provided.  Example:
 * </p>
 * <pre>
 *     readerThreads[i] = S3LogReader.builder()
 *                     .s3Client(s3Client)
 *                     .logBucket(logBucket)
 *                     .keyList(syncKeyList)
 *                     .logLines(logLines)
 *                     .killer(killer)
 *                     .build();
 * </pre>
 * @author Ian Kaplan, Topstone Software Consulting
 */
@Slf4j
@Builder
@AllArgsConstructor
public class S3LogReader implements Runnable {
    private static final String S3_ID = "AWS_ACCESS_KEY_ID";
    private static final String S3_KEY = "AWS_SECRET_ACCESS_KEY";
    private static final Regions S3_REGION = Regions.US_WEST_1;
    private static final Logger logger = LoggerFactory.getLogger(S3LogReader.class);
    private static final AtomicInteger idGen = new AtomicInteger();
    private final int threadID = idGen.incrementAndGet();
    @NotNull
    private final AmazonS3 s3Client;
    @NotNull
    private final String logBucket;
    @NotNull
    private final S3KeyList keyList;
    @NotNull
    private final LinkedBlockingQueue<String> logLines;
    @NotNull
    private final Killer killer;

    @SneakyThrows
    @Override
    public void run() {
        int linesProcessed = 0;
        killer.register(threadID);
        Optional<String> key;
        try {
            while ((key = keyList.getKey()).isPresent()) {
                String keyVal = key.get();
                InputStream istream = s3Client.getObject(logBucket, keyVal).getObjectContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(istream));
                String logLine;
                while ((logLine = reader.readLine()) != null) {
                    if (! logLine.isBlank()) {
                        logLines.put(logLine);
                        linesProcessed++;
                    }
                }
            }
        } catch (IOException e) {
            String msg = "run: " + e.getLocalizedMessage();
            logger.error(msg);
        } catch(InterruptedException e) {
            logger.error("Thread interrupted");
            Thread.currentThread().interrupt();
        }
        killer.removeID(threadID, linesProcessed);
    }

}
