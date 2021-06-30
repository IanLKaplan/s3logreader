package com.topstonesoftware.s3logreader;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Build an AmazonS3 client
 */
public class S3ClientBuilder {

    private S3ClientBuilder() {}

    public static AmazonS3 getS3Client(String id, String key, String regionStr, int maxConnections) {
            AWSCredentials credentials = new BasicAWSCredentials(id, key);
            AWSCredentialsProvider credProvider = new AWSStaticCredentialsProvider( credentials );
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setMaxConnections( maxConnections );
            return AmazonS3ClientBuilder.standard().withRegion(regionStr).withCredentials( credProvider ).withClientConfiguration(clientConfig).build();
    }

}
