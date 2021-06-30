/*
    This software is published under the Apache 2 software license.
 */

package com.topstonesoftware.s3logreader;

import org.apache.orc.TypeDescription;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *     Parse an S3 web log line, using a regular expression parser.  The regular expression is from the AWS
 *     documentation (see below).
 * </p>
 * <p>
 * @author Ian Kaplan, Topstone Software Consulting.
 * </p>
 * <p>
 * From https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/
 * </p>
 * <pre>
 *     CREATE EXTERNAL TABLE `s3_access_logs_db.mybucket_logs`(
 *   `bucketowner` STRING,
 *   `bucket_name` STRING,
 *   `requestdatetime` STRING,
 *   `remoteip` STRING,
 *   `requester` STRING,
 *   `requestid` STRING,
 *   `operation` STRING,
 *   `key` STRING,
 *   `request_uri` STRING,
 *   `httpstatus` STRING,
 *   `errorcode` STRING,
 *   `bytessent` BIGINT,
 *   `objectsize` BIGINT,
 *   `totaltime` STRING,
 *   `turnaroundtime` STRING,
 *   `referrer` STRING,
 *   `useragent` STRING,
 *   `versionid` STRING,
 *   `hostid` STRING,
 *   `sigv` STRING,
 *   `ciphersuite` STRING,
 *   `authtype` STRING,
 *   `endpoint` STRING,
 *   `tlsversion` STRING)
 * ROW FORMAT SERDE
 *   'org.apache.hadoop.hive.serde2.RegexSerDe'
 * WITH SERDEPROPERTIES (
 *   'input.regex'='([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$')
 * STORED AS INPUTFORMAT
 *   'org.apache.hadoop.mapred.TextInputFormat'
 * OUTPUTFORMAT
 *   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
 * LOCATION
 *   's3://awsexamplebucket1-logs/prefix/'
 * </pre>
 */
public class LogLineParser {
    private static final String LOG_REGEX = "([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$";
    private final Pattern pattern = Pattern.compile(LOG_REGEX);
    // [16/Apr/2021:23:15:06 +0000]
    private static final String REQUEST_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss";
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat(REQUEST_DATE_FORMAT);

    protected Map<LogFieldEnum, String> parseLine(String line) {
        EnumMap<LogFieldEnum, String> fieldMap = new EnumMap<>(LogFieldEnum.class);
        if (! line.isEmpty()) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                fieldMap.put(LogFieldEnum.BUCKET_NAME, matcher.group(LogFieldEnum.BUCKET_NAME.getFieldNum()));
                fieldMap.put(LogFieldEnum.REQUEST_DATE_TIME, matcher.group(LogFieldEnum.REQUEST_DATE_TIME.getFieldNum()));
                fieldMap.put(LogFieldEnum.REMOTE_IP, matcher.group(LogFieldEnum.REMOTE_IP.getFieldNum()));
                fieldMap.put(LogFieldEnum.OPERATION, matcher.group(LogFieldEnum.OPERATION.getFieldNum()));
                fieldMap.put(LogFieldEnum.KEY, matcher.group(LogFieldEnum.KEY.getFieldNum()));
                fieldMap.put(LogFieldEnum.REQUEST_URI, matcher.group(LogFieldEnum.REQUEST_URI.getFieldNum()));
                fieldMap.put(LogFieldEnum.HTTP_STATUS, matcher.group(LogFieldEnum.HTTP_STATUS.getFieldNum()));
                fieldMap.put(LogFieldEnum.TOTAL_TIME, matcher.group(LogFieldEnum.TOTAL_TIME.getFieldNum()));
                fieldMap.put(LogFieldEnum.REFERRER, matcher.group(LogFieldEnum.REFERRER.getFieldNum()));
                fieldMap.put(LogFieldEnum.USER_AGENT, matcher.group(LogFieldEnum.USER_AGENT.getFieldNum()));
                fieldMap.put(LogFieldEnum.VERSION_ID, matcher.group(LogFieldEnum.VERSION_ID.getFieldNum()));
                fieldMap.put(LogFieldEnum.END_POINT, matcher.group(LogFieldEnum.END_POINT.getFieldNum()));
            }
        }
        return fieldMap;
    }


    /**
     * Convert a REQUEST_DATE_TIME value to a Timestamp object
     * <pre>
     *     [16/Apr/2021:23:15:06 +0000]
     * </pre>
     * @param requestDate a date string from the log line
     * @return the date as a Timestamp object
     */
    private Timestamp convertDate(String requestDate) throws ParseException {
        Timestamp timestamp = null;
        if (requestDate != null && requestDate.length() > 0) {
            int leftBracketIx = requestDate.indexOf('[') + 1;
            int plusIx = requestDate.indexOf('+');
            requestDate = requestDate.substring(leftBracketIx, plusIx).trim();
            Date date = dateFormatter.parse(requestDate);
            timestamp = new Timestamp(date.getTime());
        }
        return timestamp;
    }


    /**
     * Convert the map that was built from a log file line into a list of objects that can be written out
     * to the ORC file.  Note that the ordering in the list matches the ordering of the objects in the
     * schema.
     *
     * @param columnMap the map that was built from the log file line
     * @return a list of objects that can be written out as an ORC file row.
     */
    protected List<Object> buildOrcRow(Map<LogFieldEnum, String> columnMap) throws ParseException {
        List<Object> row = new ArrayList<>();
        if (! columnMap.isEmpty()) {
            row.add(columnMap.get(LogFieldEnum.BUCKET_NAME));
            String requestDateTime = columnMap.get(LogFieldEnum.REQUEST_DATE_TIME);
            Timestamp timestamp = convertDate(requestDateTime);
            row.add(timestamp);
            row.add(columnMap.get(LogFieldEnum.REMOTE_IP));
            row.add(columnMap.get(LogFieldEnum.OPERATION));
            row.add(columnMap.get(LogFieldEnum.KEY));
            row.add(columnMap.get(LogFieldEnum.REQUEST_URI));
            row.add(Integer.valueOf(columnMap.get(LogFieldEnum.HTTP_STATUS)));
            row.add(Integer.valueOf(columnMap.get(LogFieldEnum.TOTAL_TIME)));
            row.add(columnMap.get(LogFieldEnum.REFERRER));
            row.add(columnMap.get(LogFieldEnum.USER_AGENT));
            row.add(columnMap.get(LogFieldEnum.VERSION_ID));
            row.add(columnMap.get(LogFieldEnum.END_POINT));
        }
        return row;
    }

    /**

     * <pre>
     *  + BUCKET_NAME       hotbottomstories.com
     *  + REQUEST_DATE_TIME [16/Apr/2021:23:15:06 +0000]
     *  + REMOTE_IP         102.159.89.150
     *  + OPERATION         WEBSITE.GET.OBJECT
     *  + KEY               m_f_stories/index.html
     *  + REQUEST_URI       "GET /m_f_stories/ HTTP/1.1"
     *  + HTTP_STATUS       200
     *  + TOTAL_TIME        32
     *  + REFERRER          "https://www.google.com/"
     *  + USER_AGENT        "Mozilla/5.0 (Linux; Android 8.1.0; DRA-LX5) AppleWebKit/537.36
     *  + VERSION_ID        (KHTML, like Gecko) Chrome/90.0.4430.66 Mobile Safari/537.36"
     *  + END_POINT         hotbottomstories.com
     * </pre>
     * @return return a schema for a processed log entry.
     */
    public static TypeDescription buildOrcFileSchema() {
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField(LogFieldEnum.BUCKET_NAME.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.REQUEST_DATE_TIME.getFieldName(), TypeDescription.createTimestamp());
        schema.addField(LogFieldEnum.REMOTE_IP.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.OPERATION.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.KEY.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.REQUEST_URI.getFieldName(), TypeDescription.createString() );
        schema.addField(LogFieldEnum.HTTP_STATUS.getFieldName(), TypeDescription.createInt());
        schema.addField(LogFieldEnum.TOTAL_TIME.getFieldName(), TypeDescription.createInt());
        schema.addField(LogFieldEnum.REFERRER.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.USER_AGENT.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.VERSION_ID.getFieldName(), TypeDescription.createString());
        schema.addField(LogFieldEnum.END_POINT.getFieldName(), TypeDescription.createString());
        return schema;
    }

    public List<Object> processLogfileLine(String line) throws ParseException {
        Map<LogFieldEnum, String> columnMap = parseLine( line );
        return buildOrcRow( columnMap);
    }

}
