package com.topstonesoftware.aws_s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 * Read an S3 directory as a potentially paginated list of S3 keys (e.g., S3 file paths).
 * </p>
 * <p>
 *     S3 directories can be very large. The directory cannot be listed in a single operation because
 *     of memory limitations and network performance (S3 is an HTTP resource). This class allows blocks
 *     of directory names to be read so that the entire directory can be read in a paginated fashion.
 * </p>
 * <p>
 *     The S3 directory structure may consist of a set of prefixes (logical sub-directories):
 * </p>
 * <pre>
 *     foo/
 *         my_file_1
 *         my_file_2
 *     bar/
 *         your_file_1
 *         your_file_2
 * </pre>
 * <p>
 *     Here the prefixes are "foo" and "bar"  If the prefix is provided, only the S3 paths that include tht prefix
 *     will be included in the "directory" list.
 * </p>
 * <p>
 *     This class should be called once for a given S3 bucket and prefix.
 * </p>
 */
public class S3DirectoryList {
    private final AmazonS3 amazonS3;
    private final String bucket;
    private final String prefix;
    private String startAfter = null;

    /**
     *
     * @param amazonS3 The authenticated AmazonS3 client
     * @param bucket The bucket to be listed.
     * @param prefix The prefix within the bucket. If no prefix is needed the prefix should be the
     *               empty string "".
     */
    public S3DirectoryList(AmazonS3 amazonS3, String bucket, String prefix) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    /**
     * Return a list of S3 file names for the bucket/prefix.
     *
     * @param numToRead the maximum number of S3 paths to return.
     * @return a list of S3 directory paths. If all of the paths have been read, then a list of length
     *         zero will be returned.
     */
    public List<String> listDirectory(final int numToRead) {
        List<String> keyList = new ArrayList<>();
        ListObjectsV2Result listObjects;
        int numRemaining = numToRead;
        do {
            var request = new ListObjectsV2Request();
            request.setBucketName(bucket);
            request.setPrefix(prefix);
            request.setMaxKeys( numToRead );
            if (startAfter != null) {
                request.setStartAfter( startAfter);
            }

            listObjects = amazonS3.listObjectsV2(request);
            List<String> filePathList = listObjects.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
            keyList.addAll(filePathList);
            numRemaining = numRemaining - filePathList.size();
            if (! filePathList.isEmpty()) {
                startAfter = filePathList.get(filePathList.size() - 1);
            }
        } while (listObjects.isTruncated() && numRemaining > 0);
        return keyList;
    }

}

