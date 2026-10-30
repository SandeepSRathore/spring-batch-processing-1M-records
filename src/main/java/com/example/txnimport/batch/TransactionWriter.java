package com.example.txnimport.batch;

import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.example.txnimport.batch.RejectionRecorder.ReasonCode;
import com.example.txnimport.batch.RejectionRecorder.Rejection;

import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Batch-inserts a chunk with {@code ON CONFLICT DO NOTHING} and records rows that hit an
 * existing transaction_id as {@code DUPLICATE} rejections, in the same transaction.
 *
 * <p>Handling duplicates here, rather than throwing and letting Spring Batch skip them,
 * keeps the chunk on the fast path: a write exception would trigger a rollback and an
 * item-by-item rescan of the whole chunk.
 */
public class TransactionWriter implements ItemWriter<Transaction> {

    private static final String INSERT_SQL = """
            INSERT INTO customer_transaction
                (transaction_id, customer_id, amount, currency, type, occurred_at, merchant, status,
                 source_file, source_line, job_instance_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;
    private final RejectionRecorder rejectionRecorder;

    public TransactionWriter(JdbcTemplate jdbcTemplate, RejectionRecorder rejectionRecorder) {
        this.jdbcTemplate = jdbcTemplate;
        this.rejectionRecorder = rejectionRecorder;
    }

    @Override
    public void write(Chunk<? extends Transaction> chunk) {
        StepExecution stepExecution = ImportJobParameters.currentStepExecution();
        String sourceFile = ImportJobParameters.sourceFile(stepExecution);
        long jobInstanceId = stepExecution.getJobExecution().getJobInstanceId();
        List<? extends Transaction> items = chunk.getItems();

        int[][] updateCounts = jdbcTemplate.batchUpdate(INSERT_SQL, items, items.size(), (ps, txn) -> {
            ps.setString(1, txn.transactionId());
            ps.setString(2, txn.customerId());
            ps.setBigDecimal(3, txn.amount());
            ps.setString(4, txn.currency());
            ps.setString(5, txn.type().name());
            ps.setTimestamp(6, Timestamp.from(txn.occurredAt().toInstant()));
            ps.setString(7, txn.merchant());
            ps.setString(8, txn.status().name());
            ps.setString(9, sourceFile);
            ps.setLong(10, txn.sourceLine());
            ps.setLong(11, jobInstanceId);
        });

        List<Rejection> duplicates = new ArrayList<>();
        int index = 0;
        for (int[] batch : updateCounts) {
            for (int count : batch) {
                if (count == Statement.SUCCESS_NO_INFO) {
                    // Happens with the pgjdbc reWriteBatchedInserts option: duplicates would go unnoticed.
                    throw new IllegalStateException("JDBC driver did not report per-row update counts; "
                            + "duplicate detection needs them (is reWriteBatchedInserts enabled?)");
                }
                if (count == 0) {
                    Transaction txn = items.get(index);
                    duplicates.add(new Rejection(txn.sourceLine(), txn.rawLine(), ReasonCode.DUPLICATE,
                            "transaction_id %s already exists".formatted(txn.transactionId())));
                }
                index++;
            }
        }

        rejectionRecorder.recordAll(stepExecution, duplicates);
    }
}
