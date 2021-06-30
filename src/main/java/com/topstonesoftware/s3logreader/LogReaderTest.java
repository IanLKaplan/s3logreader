package com.topstonesoftware.s3logreader;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main program for code to convert S3 web logs to ORC files
 */
@Slf4j
public class LogReaderTest {
    private static final Logger logger = LoggerFactory.getLogger(LogReaderTest.class);
    private static final String LOG_BUCKET = "bearcave.logs";
    private static final String ORC_BUCKET = "ianlkaplan-logs.orc";
    private static final String PREFIX = "";
    private static final String ORC_PATH_PREFIX = "http_logs";
    private static final String ORC_FILE_PREFIX = "bearcave";


    public static void main(String[] argv) {
        LogsToOrc logsToOrc = LogsToOrc.builder()
                .logBucket(LOG_BUCKET)
                .logPathPrefix(PREFIX)
                .orcBucket(ORC_BUCKET)
                .orcPathPrefix(ORC_PATH_PREFIX)
                .orcFilePrefix(ORC_FILE_PREFIX)
                .build();
        try {
            logsToOrc.processLogFiles();
        } catch (LogReaderException e) {
            logger.error(e.getLocalizedMessage());
        }
        System.exit(0);
    }
}
