
/*
  This software is published under the Apache 2 software license
 */
package com.topstonesoftware.s3logreader;

import com.topstonesoftware.javaorc.ORCFileException;
import com.topstonesoftware.javaorc.WriteORCFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Read S3 web log file lines from a LinkedBlockingQueue, convert the lines to ORC row format and write
 * the lines out to an ORC file.
 *
 * @author Ian Kaplan, Topstone Software Consulting
 */
@Slf4j
public class BatchToOrc implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BatchToOrc.class);
    private static final TypeDescription schema = LogLineParser.buildOrcFileSchema();
    private final Configuration writerConfig = new Configuration();
    private final String orcBucket;
    private final String orcPathPrefix;
    private final String orcFilename;
    private final LinkedBlockingQueue<String> logLineQueue;
    private int linesProcessed = 0;

    public BatchToOrc(String orcBucket, String orcPathPrefix, String orcFilename, LinkedBlockingQueue<String> logLineQueue) {
        this.orcBucket = orcBucket;
        this.orcPathPrefix = orcPathPrefix;
        this.orcFilename = orcFilename;
        this.logLineQueue = logLineQueue;
    }

    public int getLinesProcessed() {
        return linesProcessed;
    }

    private FileSystem buildFileSystem() throws URISyntaxException, IOException {
        FileSystem fileSystem = new S3AFileSystem();
        String uriStr = "s3://" + orcBucket;
        fileSystem.initialize(new URI(uriStr), writerConfig);
        return fileSystem;
    }

    private Writer buildWriter(FileSystem fileSystem, String key) throws IOException {
        Path hadoopPath = new Path( key );
        return OrcFile.createWriter(hadoopPath,
                OrcFile.writerOptions(writerConfig)
                .fileSystem( fileSystem)
                .setSchema(BatchToOrc.schema)
                .overwrite(true));
    }


    private void processLine(WriteORCFile orcFileWriter, LogLineParser parser, String line) throws ORCFileException {
        try {
            List<Object> row = parser.processLogfileLine(line);
            if (! row.isEmpty()) {
                orcFileWriter.writeRow(row);
                linesProcessed++;
            }
        } catch (ParseException e) {
            logger.error("run: log parsing error");
        }
    }

    @Override
    public void run() {
        try {
            String orcFilePath = orcPathPrefix + "/" + orcFilename;
            FileSystem s3FileSystem = buildFileSystem();
            Writer fileWriter = buildWriter(s3FileSystem, orcFilePath);
            try (WriteORCFile orcFileWriter = new WriteORCFile("bogus", schema)) {
                orcFileWriter.setOrcWriter(fileWriter); // set the S3 ORC writer
                LogLineParser parser = new LogLineParser();
                boolean processingBatch = true;
                while (processingBatch) {
                    try {
                        String line = logLineQueue.take();
                        processLine(orcFileWriter, parser, line);
                    } catch (InterruptedException e) {
                        if (! logLineQueue.isEmpty()) {
                            List<String> remainingElem = new ArrayList<>();
                            logLineQueue.drainTo(remainingElem);
                            for (String line : remainingElem) {
                                processLine(orcFileWriter, parser, line);
                            }
                        }
                        processingBatch = false;
                    }
                } // while
            } catch (ORCFileException e) {
                logger.error("run: {}", e.getLocalizedMessage());
            } // orcFileWriter
        } catch (URISyntaxException | IOException e) {
            logger.error("run: {}", e.getLocalizedMessage());
        }
    }

}
