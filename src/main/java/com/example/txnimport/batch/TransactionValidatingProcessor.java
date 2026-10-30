package com.example.txnimport.batch;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import com.example.txnimport.batch.Transaction.TransactionStatus;
import com.example.txnimport.batch.Transaction.TransactionType;

import org.springframework.batch.infrastructure.item.ItemProcessor;

/**
 * Validates and normalises one row. All rule violations of a row are collected so the
 * rejection record explains everything that is wrong with it, not only the first problem.
 */
public class TransactionValidatingProcessor implements ItemProcessor<TransactionCsvRow, Transaction> {

    static final BigDecimal MAX_AMOUNT = new BigDecimal("1000000.00");
    /** Tolerated clock skew between the producer of the file and this service. */
    static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);
    static final int MAX_MERCHANT_LENGTH = 100;

    private static final Pattern TRANSACTION_ID = Pattern.compile("[A-Z0-9][A-Z0-9-]{0,39}");
    private static final Pattern CUSTOMER_ID = Pattern.compile("CUST-\\d{6}");

    private final Clock clock;

    public TransactionValidatingProcessor(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Transaction process(TransactionCsvRow row) {
        List<String> violations = new ArrayList<>();

        String transactionId = upper(row.transactionId());
        if (transactionId == null) {
            violations.add("transaction_id is required");
        } else if (!TRANSACTION_ID.matcher(transactionId).matches()) {
            violations.add("transaction_id '%s' has an invalid format".formatted(row.transactionId()));
        }

        String customerId = upper(row.customerId());
        if (customerId == null) {
            violations.add("customer_id is required");
        } else if (!CUSTOMER_ID.matcher(customerId).matches()) {
            violations.add("customer_id '%s' must look like CUST-123456".formatted(row.customerId()));
        }

        BigDecimal amount = parseAmount(row.amount(), violations);
        String currency = parseCurrency(row.currency(), violations);
        TransactionType type = parseEnum(TransactionType.class, "type", row.type(), violations);
        OffsetDateTime occurredAt = parseTimestamp(row.timestamp(), violations);
        TransactionStatus status = parseEnum(TransactionStatus.class, "status", row.status(), violations);

        String merchant = trimToNull(row.merchant());
        if (merchant != null && merchant.length() > MAX_MERCHANT_LENGTH) {
            violations.add("merchant is longer than %d characters".formatted(MAX_MERCHANT_LENGTH));
        }
        if (type == TransactionType.DEBIT && merchant == null) {
            violations.add("merchant is required for DEBIT transactions");
        }

        if (!violations.isEmpty()) {
            throw new InvalidTransactionException(violations);
        }
        return new Transaction(transactionId, customerId, amount, currency, type, occurredAt, merchant, status,
                row.lineNumber(), row.rawLine());
    }

    private static BigDecimal parseAmount(String raw, List<String> violations) {
        String value = trimToNull(raw);
        if (value == null) {
            violations.add("amount is required");
            return null;
        }
        BigDecimal amount;
        try {
            amount = new BigDecimal(value);
        } catch (NumberFormatException e) {
            violations.add("amount '%s' is not a number".formatted(value));
            return null;
        }
        if (amount.signum() <= 0) {
            violations.add("amount must be positive but was %s".formatted(value));
        } else if (amount.compareTo(MAX_AMOUNT) > 0) {
            violations.add("amount %s exceeds the maximum of %s".formatted(value, MAX_AMOUNT));
        } else if (amount.stripTrailingZeros().scale() > 2) {
            violations.add("amount %s has more than 2 decimal places".formatted(value));
        }
        return amount;
    }

    private static String parseCurrency(String raw, List<String> violations) {
        String code = upper(raw);
        if (code == null) {
            violations.add("currency is required");
            return null;
        }
        try {
            return Currency.getInstance(code).getCurrencyCode();
        } catch (IllegalArgumentException e) {
            violations.add("currency '%s' is not an ISO 4217 code".formatted(raw.trim()));
            return null;
        }
    }

    private OffsetDateTime parseTimestamp(String raw, List<String> violations) {
        String value = trimToNull(raw);
        if (value == null) {
            violations.add("timestamp is required");
            return null;
        }
        OffsetDateTime timestamp;
        try {
            timestamp = OffsetDateTime.parse(value);
        } catch (DateTimeParseException e) {
            violations.add("timestamp '%s' is not ISO-8601 with an offset (e.g. 2026-01-31T13:45:00Z)".formatted(value));
            return null;
        }
        if (timestamp.isAfter(OffsetDateTime.now(clock).plus(MAX_FUTURE_SKEW))) {
            violations.add("timestamp %s is in the future".formatted(value));
        }
        return timestamp;
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String field, String raw, List<String> violations) {
        String value = upper(raw);
        if (value == null) {
            violations.add(field + " is required");
            return null;
        }
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            violations.add("%s '%s' is not one of %s".formatted(field, raw.trim(), List.of(type.getEnumConstants())));
            return null;
        }
    }

    private static String upper(String raw) {
        String value = trimToNull(raw);
        return value == null ? null : value.toUpperCase(Locale.ROOT);
    }

    private static String trimToNull(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.strip();
        return value.isEmpty() ? null : value;
    }
}
