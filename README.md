# s3logreader

An application that reads S3 web log files, stored on S3, and processes the logs into ORC files.

## S3 Web Server Logfile Analysis

Web sites hosted on Amazon Web Services S3 are low cost and support very high bandwidth (Athough there are [probably limits](https://www.theguardian.com/lifeandstyle/2014/dec/17/kim-kardashian-butt-break-the-internet-paper-magazine)). 

AWS allows logging to be turned on for a web site. The web log files are stored in a separate bucket. For example, the web logs for 
[topstonesoftware.com](www.topstonesoftware.com) are stored in a bucket that I named topstonesoftware.logs. Logs that are older than
a defined time period can be removed so that S3 storage space is not consumed without limit.

Unfortunately there is no inexpensive way to analyze the log data that is collected for your S3 web sites.  The log data can be exported to
an Elastic Search Service instance, which can be used to do log analysis. The smallest micro Elastic Search Service instance currently costs approximately $13.14
per month, which is more than I pay for all of my web sites. 

AWS Athena is an interactive query service where you pay on a per-query basis. Athena uses standard SQL to query data.  Amazon has published an example showing how
Athena can be used to query log file data (see [How do I analyze my Amazon S3 server access logs using Athena?](https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/)). Queries against raw log data are, unfortunately, very slow, since Athena will process all of the log data for each query.  

Unlike a relational database, Athena scans all of the data.  

![alt Diagram for S3 log files to ORC](https://github.com/IanLKaplan/s3logreader/blob/master/img/s3_logs_to_orc_diagram.png?raw=true)
