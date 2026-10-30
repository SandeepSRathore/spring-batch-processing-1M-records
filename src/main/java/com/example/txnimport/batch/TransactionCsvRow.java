package com.example.txnimport.batch;

/**
 * One CSV line exactly as read: every field is still a string so that type errors are
 * reported by the validator with a useful message instead of failing the read.
 */
public record TransactionCsvRow(
        long lineNumber,
        String rawLine,
        String transactionId,
        String customerId,
        String amount,
        String currency,
        String type,
        String timestamp,
        String merchant,
        String status) {
}
