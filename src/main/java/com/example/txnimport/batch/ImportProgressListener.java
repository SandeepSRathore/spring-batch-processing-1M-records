package com.example.txnimport.batch;

import java.time.Duration;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.ChunkListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;

/** Periodic progress lines and a per-run summary, so a long import is never a black box. */
public class ImportProgressListener implements ChunkListener<TransactionCsvRow, Transaction>, StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ImportProgressListener.class);

    /** Saved by the reader named "transactionReader" at every commit; present only when resuming. */
    private static final String READER_POSITION_KEY = "transactionReader.read.count";

    private final int logIntervalChunks;

    public ImportProgressListener(int logIntervalChunks) {
        this.logIntervalChunks = logIntervalChunks;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Import step starting: jobExecutionId={}, file={}, resuming={}",
                stepExecution.getJobExecutionId(), ImportJobParameters.sourceFile(stepExecution),
                stepExecution.getExecutionContext().containsKey(READER_POSITION_KEY));
    }

    @Override
    public void afterChunk(Chunk<Transaction> chunk) {
        StepExecution step = ImportJobParameters.currentStepExecution();
        // The commit count is incremented right after this callback, hence the + 1.
        long chunks = step.getCommitCount() + 1;
        if (chunks % logIntervalChunks == 0) {
            log.info("Import progress: jobExecutionId={}, read={}, written={}, skipped={}, rate={} rows/s",
                    step.getJobExecutionId(), step.getReadCount(), step.getWriteCount(), step.getSkipCount(),
                    rowsPerSecond(step));
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution step) {
        log.info("Import step finished: jobExecutionId={}, status={}, read={}, written(incl. duplicates)={}, "
                        + "readSkips={}, processSkips={}, commits={}, rollbacks={}, rate={} rows/s",
                step.getJobExecutionId(), step.getStatus(), step.getReadCount(), step.getWriteCount(),
                step.getReadSkipCount(), step.getProcessSkipCount(), step.getCommitCount(), step.getRollbackCount(),
                rowsPerSecond(step));
        return step.getExitStatus();
    }

    private static long rowsPerSecond(StepExecution step) {
        LocalDateTime start = step.getStartTime();
        if (start == null) {
            return 0;
        }
        long millis = Math.max(1, Duration.between(start, LocalDateTime.now()).toMillis());
        long rows = step.getReadCount() + step.getReadSkipCount();
        return rows * 1000 / millis;
    }
}
