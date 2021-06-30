/*
  This software is published under the Apache 2 software license.
 */

package com.topstonesoftware.s3logreader;

/**
 * An enumeration whose field value corresponds to the fields in the RegEx parsed S3 web log line.
 *
 * @author Ian Kaplan, Topstone Software Consulting
 */
public enum LogFieldEnum {
    BUCKET_OWNER("bucket_owner", 1),
    BUCKET_NAME("bucket_name", 2),
    REQUEST_DATE_TIME("request_date", 3),
    REMOTE_IP("remote_ip", 4),
    REQUESTER("requester", 5),
    REQUEST_ID("request_id", 6),
    OPERATION("operation", 7),
    KEY("key", 8),
    REQUEST_URI("request_uri", 9),
    HTTP_STATUS("http_status", 10),
    ERROR_CODE("error_code", 11),
    BYTES_SENT("bytes_sent", 12),
    OBJECT_SIZE("object_size", 13),
    TOTAL_TIME("total_time", 14),
    TURNAROUND_TIME("turnaround_time", 15),
    REFERRER("referrer", 16),
    USER_AGENT("user_agent", 17),
    VERSION_ID("version_id", 18),
    HOST_ID("host_id", 19),
    SIGV("sigv", 20),
    CIPHER_SUITE("cypher_suite", 21),
    AUTH_TYPE("auth_type", 22),
    END_POINT("end_point", 23),
    TLS_VERSION("tls_version", 24);

    private final int fieldNum;
    private final String name;

    LogFieldEnum(String name,  int field ) {
        this.name = name;
        this.fieldNum = field;

    }
    public int getFieldNum() { return fieldNum; }
    public String getFieldName() { return name; };
}
