package com.topstonesoftware.s3logreader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The main program for code to convert S3 web logs to ORC files
 * </p>
 * <h4>
 *     Command line arguments:
 * </h4>
 * <ul>
 *     <li>--logBucket [S3 bucket name for the S3 web logs files]</li>
 *     <li>--orcBucket [S3 bucket name for the ORC files]</li>
 *     <li>--orcPathPrefix [the prefix for the S3 path. For example: http_logs </li>
 *     <li>--logPathPrefix [an optional path prefix for the S3 log files]</li>
 *     <li>--domainName [the name of the domain for the S3 web logs. E.g., example.com]</li>
 *     <li>--help [print the command line arguments]</li>
 * </ul>
 * <pre>
 *     --domainName example.com --logBucket example.logs --orcBucket ianlkaplan-logs.orc --orcPathPrefix http_logs
 * </pre>
 *
 */
@Slf4j
public class LogReaderMain {
    private static final Logger logger = LoggerFactory.getLogger(LogReaderMain.class);
    private static final String LOG_BUCKET_CL = "logBucket";
    private static final String ORC_BUCKET_CL = "orcBucket";
    private static final String ORC_PATH_PREFIX_CL = "orcPathPrefix";
    private static final String LOG_PATH_PREFIX_CL = "logPathPrefix";
    private static final String DOMAIN_CL = "domainName";
    private static final String HELP_CL = "help";

    private static Options buildOptions() {
        Options options = new Options();
        Option logBucketOpt = Option.builder()
                .longOpt( LOG_BUCKET_CL )
                .hasArg()
                .desc("The S3 bucket containing the S3 web access log files")
                .required()
                .build();
        options.addOption( logBucketOpt);
        Option orcBucketOpt = Option.builder()
                .longOpt( ORC_BUCKET_CL)
                .hasArg()
                .desc("The S3 bucket for the ORC files that are generated from the S3 log files")
                .required()
                .build();
        options.addOption(orcBucketOpt);
        Option orcPathPrefix = Option.builder()
                .longOpt(ORC_PATH_PREFIX_CL)
                .hasArg()
                .desc("An optional S3 path prefix for the ORC files. Example: http_logs")
                .required(false)
                .build();
        options.addOption(orcPathPrefix);
        Option logPathPrefixOpt = Option.builder()
                .longOpt( LOG_PATH_PREFIX_CL )
                .hasArg()
                .desc("An optional S3 path prefix for the log files")
                .required(false)
                .build();
        options.addOption(logPathPrefixOpt);
        Option logDomainNameOpt = Option.builder()
                .longOpt( DOMAIN_CL)
                .hasArg()
                .desc("The domain name that the web logs were collected for.")
                .required()
                .build();
        options.addOption(logDomainNameOpt);
        Option helpOpt = Option.builder()
                .longOpt( HELP_CL )
                .hasArg(false)
                .desc("Display the command line options")
                .required( false )
                .build();
        options.addOption(helpOpt);
        return options;
    }

    public static void help(Options cliOptions) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "s3logreader", cliOptions );
    }

    public static void main(String[] args) {
        Options cliOptions = LogReaderMain.buildOptions();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(cliOptions, args);

            if (commandLine.hasOption("help") ) {
                help(cliOptions);
            } else {
                String logBucket = "";
                if (commandLine.hasOption(LOG_BUCKET_CL)) {
                    logBucket = commandLine.getOptionValue( LOG_BUCKET_CL );
                }
                String orcBucket = "";
                if (commandLine.hasOption(ORC_BUCKET_CL)) {
                    orcBucket = commandLine.getOptionValue(ORC_BUCKET_CL);
                }
                String orcPathPrefix = "";
                if (commandLine.hasOption(ORC_PATH_PREFIX_CL)) {
                    orcPathPrefix = commandLine.getOptionValue(ORC_PATH_PREFIX_CL);
                }
                String logPathPrefix = "";
                if (commandLine.hasOption(LOG_PATH_PREFIX_CL)) {
                    logPathPrefix = commandLine.getOptionValue( LOG_PATH_PREFIX_CL);
                }
                String domain = "";
                if (commandLine.hasOption(DOMAIN_CL)) {
                    domain = commandLine.getOptionValue( DOMAIN_CL );
                }
                try {
                    LogsToOrc logsToOrc = LogsToOrc.builder()
                            .logBucket(logBucket)
                            .logPathPrefix(logPathPrefix)
                            .orcBucket(orcBucket)
                            .orcPathPrefix(orcPathPrefix)
                            .logDomainName(domain)
                            .build();
                    logsToOrc.processLogFiles();
                } catch (LogReaderException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        } catch (ParseException e) {
            logger.error("Error parsing command line arguments: {}", e.getLocalizedMessage());
            help(cliOptions);
        }
        System.exit(0);
    }
}
