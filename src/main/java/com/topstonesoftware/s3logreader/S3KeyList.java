package com.topstonesoftware.s3logreader;

import java.util.List;
import java.util.Optional;

/**
 * A synchronized container for a batch of S3 web log file keys.  This Class is initialized with the key list.
 * The keys are read by the S3LogReader threads, which read the S3 files associated with the keys and write
 * the log lines read to a LinkedBlockingQueue.
 */
public class S3KeyList {
    final List<String> keyList;
    int keyIx = 0;

    public S3KeyList(List<String> keyList) {
        this.keyList = keyList;
    }

    public synchronized Optional<String> getKey() {
        Optional<String> key = Optional.empty();
        if (keyIx < keyList.size()) {
            key = Optional.of(keyList.get( keyIx ));
            keyIx++;
        }
        return key;
    }
}
