package com.example.txnimport.batch;

import com.example.txnimport.batch.RejectionRecorder.ReasonCode;
import com.example.txnimport.batch.RejectionRecorder.Rejection;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;

/**
 * Records every skipped line. Spring Batch calls these callbacks on the chunk thread inside
 * the chunk transaction, so a skip is persisted exactly when its chunk commits.
 *
 * <p>Write skips are deliberately not configured (see {@link TransactionWriter}): a write
 * skip makes Spring Batch roll the chunk back and re-scan it item by item, which would also
 * roll back the rejections already recorded for read/process skips in that chunk.
 */
public class RejectionSkipListener implements SkipListener<TransactionCsvRow, Transaction> {

    private final RejectionRecorder recorder;

    public RejectionSkipListener(RejectionRecorder recorder) {
        this.recorder = recorder;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        Rejection rejection = t instanceof FlatFileParseException parse
                ? new Rejection((long) parse.getLineNumber(), parse.getInput(), ReasonCode.PARSE_ERROR, rootMessage(t))
                : new Rejection(null, null, ReasonCode.PARSE_ERROR, rootMessage(t));
        recorder.record(ImportJobParameters.currentStepExecution(), rejection);
    }

    @Override
    public void onSkipInProcess(TransactionCsvRow row, Throwable t) {
        recorder.record(ImportJobParameters.currentStepExecution(),
                new Rejection(row.lineNumber(), row.rawLine(), ReasonCode.VALIDATION_ERROR, t.getMessage()));
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root == t ? String.valueOf(t.getMessage()) : t.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
