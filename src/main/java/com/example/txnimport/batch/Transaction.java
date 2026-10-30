package com.example.txnimport.batch;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/** A validated, normalised transaction ready to be written. */
public record Transaction(
        String transactionId,
        String customerId,
        BigDecimal amount,
        String currency,
        TransactionType type,
        OffsetDateTime occurredAt,
        String merchant,
        TransactionStatus status,
        long sourceLine,
        String rawLine) {

    public enum TransactionType { DEBIT, CREDIT }

    public enum TransactionStatus { PENDING, COMPLETED, FAILED, REVERSED }
}
