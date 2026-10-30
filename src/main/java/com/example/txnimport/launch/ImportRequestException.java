package com.example.txnimport.launch;

/** A launch/restart/stop request that cannot be honoured, with the reason category for the API. */
public class ImportRequestException extends RuntimeException {

    public enum Reason { INVALID, NOT_FOUND, CONFLICT, BUSY }

    private final Reason reason;

    public ImportRequestException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public ImportRequestException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
