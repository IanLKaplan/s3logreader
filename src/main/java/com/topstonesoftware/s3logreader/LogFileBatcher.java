/*
    This software is published under the Apache 2 software license.
 */

package com.topstonesoftware.s3logreader;

import com.amazonaws.services.s3.AmazonS3;
import com.topstonesoftware.aws_s3.S3DirectoryList;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  Read the S3 web log files in an S3 bucket and batch the files by date.  Each batch will be processed into
 *  an ORC file.
 *
 * @author Ian Kaplan, Topstone Software Consulting
 */
@Slf4j
public class LogFileBatcher {
    private static final String YEAR_MO_DAY_REGEX = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])";
    private static final Pattern pattern = Pattern.compile(YEAR_MO_DAY_REGEX);
    private static final int NAMES_TO_READ = 1000;
    private final S3DirectoryList listDir;
    private List<String> buffer = new ArrayList<>();

    public record BatchRecord(List<String> batch, String batchDate) {}

    public LogFileBatcher(AmazonS3 s3Client, String s3Bucket, String prefix) {
        listDir = new S3DirectoryList(s3Client, s3Bucket, prefix );
    }

    /**
     * <p>
     * Get a set of log file paths for one day
     * </p>
     * <p>
     *    This code assumes that all of the files in the bucket and prefix have a common
     *    file prefix, followed by a date.  For example, topstonesoftware_logs for the file name
     *    topstonesoftware_logs2021-06-17-22-12-00-800040BE6C721092
     * </p>
     * <p>
     *     This function returns a batch of files for a single day on each call.
     * </p>
     *
     * @return a one-day batch of log file paths for one day, or an empty list
     */
    public BatchRecord getLogfileBatch() {
        List<String> batch = new ArrayList<>();
        String batchDate = null;
        boolean batchDone = false;
        do {
            if (buffer.isEmpty()) {
                buffer = listDir.listDirectory(NAMES_TO_READ);
            }
            if (! buffer.isEmpty()) {
                for (String filePath : buffer) {
                    Matcher matcher = pattern.matcher(filePath);
                    if (matcher.find()) {
                        String fileDate = matcher.group();
                        if (batchDate == null) {
                            batchDate = fileDate;
                        }
                        if (fileDate.equals(batchDate)) {
                            batch.add(filePath);
                        } else {
                            batchDone = true;
                            break;
                        }
                    }
                }
                buffer.removeAll(batch);
            } else {
                batchDone = true;
            }
        } while (! batchDone);
        return new BatchRecord(batch, batchDate);
    }
}
