# s3logreader
### by Ian Kaplan, www.topstonesoftware.com, iank@bearcave.com 

An application that reads S3 web log files, stored on S3, and processes the logs into ORC files.

This application makes use of the [javaorc](https://github.com/IanLKaplan/javaorc) library in converting S3 web logfile data into ORC files.

## S3 Web Server Logfile Analysis

Web sites hosted on Amazon Web Services S3 are low cost and support very high bandwidth (athough there are [probably limits](https://www.theguardian.com/lifeandstyle/2014/dec/17/kim-kardashian-butt-break-the-internet-paper-magazine)). 

AWS allows logging to be turned on for a web site. The web log files are stored in a separate bucket. For example, the web logs for 
[topstonesoftware.com](www.topstonesoftware.com) are stored in a bucket that I named topstonesoftware.logs. Logs that are older than
a defined time period can be removed so that S3 storage space is not consumed without limit.

Unfortunately there is no inexpensive way to analyze the log data that is collected for your S3 web sites.  The log data can be exported to
an Elastic Search Service instance, which can be used to do log analysis. The smallest micro Elastic Search Service instance currently costs approximately $13.14
per month, which is more than I pay for all of my web sites. 

AWS Athena is an interactive query service where you pay on a per-query basis. Athena uses standard SQL for queries. Unlike a relational database, where queries
can make use of table indices, an Athena query will scan all of the data for every query. The per query pricing is currently $6.75 per terrabyte or 0.00675 per gigabyte of data processed.  Athena queries are rounded up to the nearest 10 megabytes, so the minimum query cost is $0.0000675.  The lost cost and on-demand nature of Athena makes it an attractive resource for data analysis.

Amazon has published an example showing how Athena can be used to query log file data (see [How do I analyze my Amazon S3 server access logs using Athena?](https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/)). Athena queries against raw log data are slow, since there are lots of small files and Athena must process all of the raw log data for every query. 

## Athena Queries Against ORC formatted log data

The performance of Athena queries against log file data can be dramatically improved by processing the S3 web server logs into ORC files. In testing against some of my web site log data the ORC compressed format reduced the data size by approximately 50 times. Athena queries against ORC data will be faster since less data must be scanned. Unless your S3 web sites have a very high traffic volumn, and a large amount of log data, the cost of Athena queries against log data converted to ORC files will be very free or very low. The AWS "Glue Catalog" (Athena) has a free tier of 1 million queries per month. In most use cases for web log analysis, this means that Athena use is free.

## Reading Logfile Data from S3

For a web site with even moderate traffic, the S3 bucket that contains the log files will have a large number of log files. The S3 keys for the bucket cannot be listed on one operation since it would be very slow and there may not be enough memory to store all of the S3 keys. This repository includes the ```aws_s3.S3directoryList``` class which will list an S3 bucket in a paginated fashion.  The S3 keys, which include a date stamp, are batched together by day and passed to the software that will read the log files. A logfile will contain one or more log lines. Each of these lines is processed into an ORC row and written out to the ORC file.  The logical processing steps are shown below.

![alt Diagram for threaded S3 log files to ORC](https://github.com/IanLKaplan/s3logreader/blob/master/img/s3_logs_to_orc_one_thread_diagram.png?raw=true)

A large number of logfiles will have to be processed. Reading the log files in a single thread, as outlined above, is so slow that the application is not useful.

AWS S3 is a massively parallel web resource. Each thread of processing for S3 is an HTTP transaction. S3 can support an almost unlimited number of HTTP threads. 
By building a multi-threaded application logfiles can be read in parallel, dramatically improving application performance. The cost of this performance is a significantly more complex application. The logical structure of the S3 log file to ORC application is shown below.

![alt Diagram for threaded S3 log files to ORC](https://github.com/IanLKaplan/s3logreader/blob/master/img/s3_logs_to_orc_diagram.png?raw=true)

## Limits on HTTP GET Operations on S3 buckets

As shown in the diagram above, the S3 log reader uses multiple Java threads to read log files.  After fetching the log file data, it is parsed and written into the ORC file.  If the bandwidth from S3 is high enough, the processing stage will become the bottleneck.

The read (HTTP GET) rate from S3 is limited currently (July 2021) to 3,500 GET operations per bucket prefex per second (see [Best practices design patterns: optimizing Amazon S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)). The log reader will probably never read data at this rate, but this limit may be an issue with data lake applications that store data in S3.

## AWS User and Permissions

When using AWS a core idea is to grant only the permissions that a particular piece of software needs. In most cases you should never run an application with your administrator permissions.

The S3 log reader only requires permissions to read and write S3.  In Amazon's Identity and Access Management console (IAM) I created a user named "s3_web_access".  I added the Amazon existing ```AmazonS3FullAccess permissions``` to this user.  The keys mentioned below are the ID and secret key for this user. 

## AWS Keys

The code in this repository relies on an AWS key, secret key and region being present as environment variables.  Your .bashrc file (or equivalent) should have the following environment variables:

```
AWS_ACCESS_KEY_ID="my key for S3 read/write access"
AWS_SECRET_ACCESS_KEY="my secret key for S3 read/write access"
AWS_REGION="the region - for example us-west-1"

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_REGION
```

This code relies on the [javaorc](https://github.com/IanLKaplan/javaorc) library.  The S3 FileSystem that is built by the ```buildFileSystem()``` function in the ```BatchToOrc``` class relies on the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables to authenticate for S3 access. 

