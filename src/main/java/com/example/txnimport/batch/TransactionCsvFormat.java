package com.example.txnimport.batch;

import java.util.List;

/** The single source of truth for the expected CSV layout. */
public final class TransactionCsvFormat {

    public static final List<String> COLUMNS = List.of(
            "transaction_id", "customer_id", "amount", "currency", "type", "timestamp", "merchant", "status");

    public static final String HEADER = String.join(",", COLUMNS);

    private static final char BYTE_ORDER_MARK = '﻿';

    private TransactionCsvFormat() {
    }

    /** Throws {@link InvalidFileException} unless {@code line} is exactly the expected header. */
    public static void requireHeader(String line) {
        String header = line;
        if (!header.isEmpty() && header.charAt(0) == BYTE_ORDER_MARK) {
            header = header.substring(1);
        }
        if (!HEADER.equals(header.strip())) {
            throw new InvalidFileException("Unexpected CSV header. Expected [%s] but was [%s]".formatted(HEADER, line));
        }
    }
}
