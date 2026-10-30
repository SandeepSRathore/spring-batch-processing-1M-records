package com.example.txnimport.batch;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import com.example.txnimport.batch.Transaction.TransactionStatus;
import com.example.txnimport.batch.Transaction.TransactionType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

class TransactionValidatingProcessorTest {

    private final TransactionValidatingProcessor processor = new TransactionValidatingProcessor(
            Clock.fixed(Instant.parse("2026-10-30T12:00:00Z"), ZoneOffset.UTC));

    @Test
    void normalisesAValidRow() {
        Transaction txn = processor.process(row(" txn-0000000001 ", "cust-000042", "12.50", "usd", "debit",
                "2026-01-31T13:45:00+05:30", "  Acme, Inc. ", "completed"));

        assertThat(txn.transactionId()).isEqualTo("TXN-0000000001");
        assertThat(txn.customerId()).isEqualTo("CUST-000042");
        assertThat(txn.amount()).isEqualByComparingTo(new BigDecimal("12.50"));
        assertThat(txn.currency()).isEqualTo("USD");
        assertThat(txn.type()).isEqualTo(TransactionType.DEBIT);
        assertThat(txn.occurredAt().toInstant()).isEqualTo(Instant.parse("2026-01-31T08:15:00Z"));
        assertThat(txn.merchant()).isEqualTo("Acme, Inc.");
        assertThat(txn.status()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(txn.sourceLine()).isEqualTo(7);
    }

    @Test
    void creditWithoutMerchantIsValid() {
        Transaction txn = processor.process(row("TXN-1", "CUST-000001", "1", "EUR", "CREDIT",
                "2026-01-01T00:00:00Z", "", "PENDING"));

        assertThat(txn.merchant()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', textBlock = """
            missing amount        | TXN-1 | CUST-000001 |          | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | amount is required
            non-numeric amount    | TXN-1 | CUST-000001 | 12.3O    | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | is not a number
            zero amount           | TXN-1 | CUST-000001 | 0        | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | must be positive
            negative amount       | TXN-1 | CUST-000001 | -5.00    | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | must be positive
            amount too large      | TXN-1 | CUST-000001 | 1000000.01 | USD | DEBIT | 2026-01-01T00:00:00Z | Shop | PENDING  | exceeds the maximum
            three decimals        | TXN-1 | CUST-000001 | 1.005    | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | more than 2 decimal places
            unknown currency      | TXN-1 | CUST-000001 | 1.00     | XYZ | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | not an ISO 4217 code
            bad type              | TXN-1 | CUST-000001 | 1.00     | USD | REFUND | 2026-01-01T00:00:00Z | Shop | PENDING   | type 'REFUND' is not one of
            bad status            | TXN-1 | CUST-000001 | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | DONE      | status 'DONE' is not one of
            bad timestamp         | TXN-1 | CUST-000001 | 1.00     | USD | DEBIT  | 31/12/2025 10:00     | Shop | PENDING   | is not ISO-8601
            timestamp no offset   | TXN-1 | CUST-000001 | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00  | Shop | PENDING   | is not ISO-8601
            future timestamp      | TXN-1 | CUST-000001 | 1.00     | USD | DEBIT  | 2026-10-30T12:06:00Z | Shop | PENDING   | is in the future
            bad customer id       | TXN-1 | C-12        | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | must look like CUST-123456
            missing transaction id|       | CUST-000001 | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | transaction_id is required
            bad transaction id    | TXN 1 | CUST-000001 | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00Z | Shop | PENDING   | has an invalid format
            debit without merchant| TXN-1 | CUST-000001 | 1.00     | USD | DEBIT  | 2026-01-01T00:00:00Z |      | PENDING   | merchant is required for DEBIT
            """)
    void rejectsInvalidRows(String description, String id, String customer, String amount, String currency,
            String type, String timestamp, String merchant, String status, String expectedMessage) {
        assertThatThrownBy(() -> processor.process(row(id, customer, amount, currency, type, timestamp, merchant, status)))
                .isInstanceOf(InvalidTransactionException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void reportsEveryViolationOfARow() {
        assertThatThrownBy(() -> processor.process(row("TXN-1", "nope", "abc", "XYZ", "DEBIT",
                "2026-01-01T00:00:00Z", "Shop", "PENDING")))
                .isInstanceOf(InvalidTransactionException.class)
                .extracting(e -> ((InvalidTransactionException) e).violations(), list(String.class))
                .hasSize(3);
    }

    @Test
    void timestampWithinClockSkewIsAccepted() {
        Transaction txn = processor.process(row("TXN-1", "CUST-000001", "1.00", "USD", "CREDIT",
                "2026-10-30T12:04:00Z", null, "PENDING"));

        assertThat(txn.occurredAt()).isNotNull();
    }

    private static TransactionCsvRow row(String id, String customer, String amount, String currency, String type,
            String timestamp, String merchant, String status) {
        return new TransactionCsvRow(7, "raw", id, customer, amount, currency, type, timestamp, merchant, status);
    }
}
