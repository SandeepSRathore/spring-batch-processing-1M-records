package com.example.txnimport.launch;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Status of one run, plus totals for the whole import (all runs of the same file), which
 * is what matters after restarts: every data line is either inserted or rejected.
 */
public record ImportStatus(
        long jobExecutionId,
        long jobInstanceId,
        String file,
        String status,
        String exitCode,
        String exitDescription,
        LocalDateTime startTime,
        LocalDateTime endTime,
        RunCounts thisRun,
        ImportTotals importTotals) {

    public record RunCounts(long read, long written, long readSkips, long processSkips, long commits, long rollbacks) {
    }

    /**
     * @param inserted rows stored in customer_transaction by this import
     * @param rejected rejections by reason code (PARSE_ERROR, VALIDATION_ERROR, DUPLICATE)
     * @param linesAccountedFor inserted + all rejections
     */
    public record ImportTotals(long inserted, Map<String, Long> rejected, long linesAccountedFor) {
    }
}
