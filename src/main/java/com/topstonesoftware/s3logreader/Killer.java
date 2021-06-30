
/*
  This software is published under the Apache 2 software license.
 */

package com.topstonesoftware.s3logreader;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 *  Reading from S3 requires multiple threads to achieve acceptable performance.  When each S3LogReader thread
 *  starts, it registers with this object. The S3LogReader threads write to a shared LinkedBlockingQueue which is
 *  read by a BatchToOrc thread.  When an S3LogReader threads terminates it deregisters. If the registration set (threadSet)
 *  is empty all S3LogReader threads have terminated and interrupt is sent to the batchToOrcThread to terminate this thread.
 *
 * @author Ian Kaplan, Topstone Software Consulting
 */
@Slf4j
public class Killer {
    private static final Logger logger = LoggerFactory.getLogger(Killer.class);
    private final Thread batchToOrcThread;
    private final Set<Integer> threadSet = new HashSet<>();
    private int totalLinesProcessed = 0;

    public Killer(Thread batchToOrcThread) {
        this.batchToOrcThread = batchToOrcThread;
    }

    public int getTotalLinesProcessed() { return totalLinesProcessed; }

    public synchronized void register(Integer threadID) {
        threadSet.add( threadID );
    }

    @SneakyThrows
    public synchronized void removeID(Integer threadID, int numLines) {
        if (threadSet.remove(threadID)) {
            totalLinesProcessed += numLines;
            if (threadSet.isEmpty()) {
                batchToOrcThread.interrupt();
            }
        } else {
            final String msg = "removeID: error, thread ID not found in set";
            logger.error(msg);
            throw new LogReaderException(msg);
        }
    }
}
