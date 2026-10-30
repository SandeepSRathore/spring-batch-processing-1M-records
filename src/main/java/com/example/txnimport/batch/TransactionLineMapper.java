package com.example.txnimport.batch;

import org.springframework.batch.infrastructure.item.file.LineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.infrastructure.item.file.transform.FieldSet;

/**
 * Tokenises a line strictly (wrong column count fails) and keeps the line number and raw
 * text so that any later rejection can point at the exact input. Any exception thrown here
 * is wrapped by the reader in a skippable {@code FlatFileParseException}.
 */
public class TransactionLineMapper implements LineMapper<TransactionCsvRow> {

    private final DelimitedLineTokenizer tokenizer;

    public TransactionLineMapper() {
        tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(TransactionCsvFormat.COLUMNS.toArray(String[]::new));
        tokenizer.setStrict(true);
    }

    @Override
    public TransactionCsvRow mapLine(String line, int lineNumber) {
        FieldSet fields = tokenizer.tokenize(line);
        return new TransactionCsvRow(
                lineNumber,
                line,
                fields.readRawString("transaction_id"),
                fields.readRawString("customer_id"),
                fields.readRawString("amount"),
                fields.readRawString("currency"),
                fields.readRawString("type"),
                fields.readRawString("timestamp"),
                fields.readRawString("merchant"),
                fields.readRawString("status"));
    }
}
