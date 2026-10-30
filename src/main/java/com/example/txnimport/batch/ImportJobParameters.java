package com.example.txnimport.batch;

import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.StepExecution;

/** Job parameter names, plus access to the step execution currently running on this thread. */
public final class ImportJobParameters {

    /** Absolute, normalised path of the CSV file. Identifying. */
    public static final String FILE = "file";
    /** SHA-256 of the file content. Identifying: same path with new content is a new import. */
    public static final String FILE_SHA256 = "fileSha256";

    private ImportJobParameters() {
    }

    public static StepExecution currentStepExecution() {
        StepContext context = StepSynchronizationManager.getContext();
        if (context == null) {
            throw new IllegalStateException("No step execution is active on this thread");
        }
        return context.getStepExecution();
    }

    public static String sourceFile(StepExecution stepExecution) {
        return stepExecution.getJobParameters().getString(FILE);
    }
}
