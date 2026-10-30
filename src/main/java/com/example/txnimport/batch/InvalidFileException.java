package com.example.txnimport.batch;

/** The input file as a whole is unusable (missing, unreadable, wrong header, changed since first run). */
public class InvalidFileException extends RuntimeException {

    public InvalidFileException(String message) {
        super(message);
    }

    public InvalidFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
