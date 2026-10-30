package com.example.txnimport.batch;

import java.sql.Types;
import java.util.List;

import org.springframework.batch.core.step.StepExecution;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Writes rejected input lines. Always called on the chunk's thread, inside the chunk
 * transaction, so a rejection is committed or rolled back together with its chunk.
 */
public class RejectionRecorder {

    public enum ReasonCode { PARSE_ERROR, VALIDATION_ERROR, DUPLICATE }

    public record Rejection(Long lineNumber, String rawLine, ReasonCode reasonCode, String reason) {
    }

    /** Lines can be arbitrarily long in a broken file; keep enough to diagnose, not the whole thing. */
    static final int MAX_STORED_LINE_LENGTH = 4000;

    private static final String INSERT_SQL = """
            INSERT INTO rejected_transaction
                (job_instance_id, job_execution_id, source_file, line_number, raw_line, reason_code, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public RejectionRecorder(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void record(StepExecution stepExecution, Rejection rejection) {
        recordAll(stepExecution, List.of(rejection));
    }

    public void recordAll(StepExecution stepExecution, List<Rejection> rejections) {
        if (rejections.isEmpty()) {
            return;
        }
        long jobInstanceId = stepExecution.getJobExecution().getJobInstanceId();
        long jobExecutionId = stepExecution.getJobExecution().getId();
        String sourceFile = ImportJobParameters.sourceFile(stepExecution);
        jdbcTemplate.batchUpdate(INSERT_SQL, rejections, rejections.size(), (ps, rejection) -> {
            ps.setLong(1, jobInstanceId);
            ps.setLong(2, jobExecutionId);
            ps.setString(3, sourceFile);
            if (rejection.lineNumber() == null) {
                ps.setNull(4, Types.BIGINT);
            } else {
                ps.setLong(4, rejection.lineNumber());
            }
            ps.setString(5, sanitize(rejection.rawLine()));
            ps.setString(6, rejection.reasonCode().name());
            ps.setString(7, sanitize(rejection.reason()));
        });
    }

    /** Postgres text columns reject NUL bytes, which do show up in corrupt files. */
    static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.replace('\u0000', '�');
        return cleaned.length() > MAX_STORED_LINE_LENGTH ? cleaned.substring(0, MAX_STORED_LINE_LENGTH) : cleaned;
    }
}
