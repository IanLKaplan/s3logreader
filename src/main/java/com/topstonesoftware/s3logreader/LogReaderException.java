package com.topstonesoftware.s3logreader;

public class LogReaderException extends Exception {
    public LogReaderException(String message) { super(message); }
    public LogReaderException(String message, Throwable cause) { super(message, cause); }
}
