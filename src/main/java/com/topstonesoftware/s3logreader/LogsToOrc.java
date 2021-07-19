
/*
     This software is licensed under the Apache 2 software license.
 */
package com.topstonesoftware.s3logreader;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>
 *     Read AWS S3 web log files and build ORC files.
 * </p>
 * <p>
 *     The Apache ORC columnar compressed file format can be used in data lake applications. The ORC file format
 *     offers the advantage of high data compression relative to JSON files or log files. This application reads
 *     AWS S3 web logs files and generates ORC files. These files cna then be read by data lake applications like
 *     AWS Athena.
 * </p>
 * <p>
 *     To instantiate this class, use the Lombok generated builder. Each of these fields is required. If the field
 *     is not set, an error will result at runtime. The order of the initializers does not matter, as long as they
 *     are all set.
 * </p>
 * <pre>
 *     LogsToOrc logsToOrc = LogsToOrc.builder()
 *                             .logBucket(logBucket)
 *                             .logPathPrefix(logPathPrefix)
 *                             .orcBucket(orcBucket)
 *                             .orcPathPrefix(orcPathPrefix)
 *                             .logDomainName(domain)
 *                             .build();
 * </pre>
 * <h4>
 *     Arguments
 * </h4>
 * <ul>
 *     <li>logBucket - the S3 bucket name that contains the S3 web log files.</li>
 *     <li>logPathPrefix - the S3 path prefix for the log files. For example, for
 *     "bearcave.logs/mylogs/something/bearcave_logs2021-06-17-22-11-26-20178924D01EF839 the logPathPrefix would be
 *     "mylogs/something"  The log path prefix may be the empty string (""). It cannot be null.
 *     </li>
 *     <li>orcBucket - the S3 bucket that that the ORC files will be written to.</li>
 *     <li>orcPathPrefix - a prefix for the ORC file path. For example: http_logs</li>
 *     <li>logDomainName - the domain that was accessed to generate the logs (e.g., example.com)</li>
 * </ul>
 * <h4>
 *     Keys and Region
 * </h4>
 * <p>
 *     This code obtains the AWS public and secret keys and the region from environment variables.
 *     This means that this code cannot (currently) be used across regions and keys.
 * </p>
 * <h4>
 *  AWS AmazonS3 Client
 * </h4>
 * <p>
 * The AmazonS3 client apparently supports multiple HTTP transactions. See the discussion:
 * https://forums.aws.amazon.com/thread.jspa?messageID=247661  Apparently is is preferred to use a single
 * cleint for multiple HTTP transactions to S3.
 * </p>
 * @author Ian L Kaplan, Topstone Software Consulting
 */

@Slf4j
@Builder
public class LogsToOrc {
    private static final String S3_ID = "AWS_ACCESS_KEY_ID";
    private static final String S3_KEY = "AWS_SECRET_ACCESS_KEY";
    // See the AWS reference https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
    private static final String S3_REGION = "AWS_REGION";
    private static final String ORC_SUFFIX = ".orc";
    private static final Logger logger = LoggerFactory.getLogger(LogsToOrc.class);
    private static final int MAX_CONNECTIONS = 64;
    private static final int NUM_THREADS = 32;
    private static final String TODAY_DATE_FORMAT = "yyyy-MM-dd";
    final SimpleDateFormat dateFormatter = new SimpleDateFormat(TODAY_DATE_FORMAT);
    @NonNull
    private final String logBucket;
    @NonNull
    private final String orcPathPrefix;
    @NonNull
    private final String logPathPrefix;
    @NonNull
    private final String orcBucket;
    @NonNull
    private final String logDomainName;

    private void launchProcessingThreads(AmazonS3 s3Client, List<String> keyList, String orcFileName) {
        S3LogReader[] readerThreads = new S3LogReader[NUM_THREADS];
        ExecutorService execPool = Executors.newFixedThreadPool( NUM_THREADS );
        S3KeyList syncKeyList = new S3KeyList(keyList);
        LinkedBlockingQueue<String> logLines = new LinkedBlockingQueue<>();
        BatchToOrc batchToOrc = new BatchToOrc(orcBucket, orcPathPrefix, logDomainName, orcFileName, logLines);
        Thread batchToOrcThread = new Thread( batchToOrc );
        Killer killer = new Killer(batchToOrcThread);
        for (int i = 0; i < NUM_THREADS; i++) {
            readerThreads[i] = S3LogReader.builder()
                    .s3Client(s3Client)
                    .logBucket(logBucket)
                    .keyList(syncKeyList)
                    .logLines(logLines)
                    .killer(killer)
                    .build();
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            execPool.execute( readerThreads[i] );
        }
        batchToOrcThread.start();
        try {
            batchToOrcThread.join();
            int killerProcessed = killer.getTotalLinesProcessed();
            int batchToOrcProcessed = batchToOrc.getLinesProcessed();
            if (killerProcessed != batchToOrcProcessed) {
                logger.error("processed lines do not match: killer processed lines: {}, batchToOrc processed lines: {}",
                        killer.getTotalLinesProcessed(), batchToOrc.getLinesProcessed());
            }
            execPool.shutdown();
        } catch (InterruptedException e) {
            // We really want to ignore this exception...
            logger.error("launchProcessingThreads: this InterruptedException should never have happened");
        }
    }

    private String getTodaysDate() {
        Date now = new Date();
        return dateFormatter.format(now);
    }

    public void processLogFiles() throws LogReaderException {
        String s3Key = System.getenv(S3_KEY);
        String s3Id = System.getenv(S3_ID);
        String s3Region = System.getenv(S3_REGION);
        if (s3Id != null && s3Key != null && s3Region != null) {
            AmazonS3Client s3Client = (AmazonS3Client)S3ClientBuilder.getS3Client(s3Id, s3Key, s3Region, MAX_CONNECTIONS);
            ClientConfiguration config = s3Client.getClientConfiguration();
            logger.info("processLogFiles: maximum AmazonS3 connections = {}", config.getMaxConnections());
            LogFileBatcher batcher = new LogFileBatcher(s3Client, logBucket, logPathPrefix);
            LogFileBatcher.BatchRecord batch;
            String todayStr = getTodaysDate();
            while (!(batch = batcher.getLogfileBatch()).batch().isEmpty()) {
                String batchDate = batch.batchDate();
                String orcFileKey = batchDate + ORC_SUFFIX;
                List<String> keyList = batch.batch();
                launchProcessingThreads(s3Client, keyList, orcFileKey);
                if (batchDate.equals(todayStr)) {
                    break; // don't process any more log files for "today" since there may be log files that arrive while the code is running
                }
            }
        } else {
            final String msg = "processLogFiles: Values for one or more of the environment variables "
                    + S3_ID + ", " + S3_KEY + ", " + S3_REGION + " not found";
            logger.error( msg );
            throw new LogReaderException( msg );
        }
    }

}
