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

AWS Athena is an interactive query service where you pay on a per-query basis. Athena uses standard SQL for queries. Unlike a relational database, where queries
can make use of table indices, an Athena query will scan all of the data for every query. The per query pricing is currently $6.75 per terrabyte or 0.00675 per gigabyte of data processed.  The lost cost and on-demand nature of Athena makes it an attractive resource for data analysis.

Amazon has published an example showing how Athena can be used to query log file data (see [How do I analyze my Amazon S3 server access logs using Athena?](https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/)). Athena queries against raw log data are slow, since Athena must process all of the log data for every query. 

## Athena Queries Against ORC formatted log data



![alt Diagram for S3 log files to ORC](https://github.com/IanLKaplan/s3logreader/blob/master/img/s3_logs_to_orc_diagram.png?raw=true)
