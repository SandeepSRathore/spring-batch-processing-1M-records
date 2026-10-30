package com.example.txnimport.launch;

public record RejectedLine(Long lineNumber, String reasonCode, String reason, String rawLine, long jobExecutionId) {
}
