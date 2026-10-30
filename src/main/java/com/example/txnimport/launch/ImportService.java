package com.example.txnimport.launch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.example.txnimport.batch.FileChecksums;
import com.example.txnimport.batch.ImportFileValidator;
import com.example.txnimport.batch.ImportJobParameters;
import com.example.txnimport.batch.InvalidFileException;
import com.example.txnimport.config.ImportProperties;
import com.example.txnimport.launch.ImportRequestException.Reason;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ImportService {

    private static final Logger log = LoggerFactory.getLogger(ImportService.class);
    private static final int MAX_EXIT_DESCRIPTION = 1000;

    private final JobOperator jobOperator;
    private final JobRepository jobRepository;
    private final Job importJob;
    private final ImportJobLifecycle lifecycle;
    private final JdbcTemplate jdbcTemplate;
    private final Path inputDir;

    public ImportService(JobOperator jobOperator, JobRepository jobRepository, Job transactionImportJob,
            ImportJobLifecycle lifecycle, JdbcTemplate jdbcTemplate, ImportProperties properties) {
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.importJob = transactionImportJob;
        this.lifecycle = lifecycle;
        this.jdbcTemplate = jdbcTemplate;
        this.inputDir = properties.inputDir();
    }

    /**
     * Starts importing {@code requestedFile} (relative to the input directory) and returns as soon
     * as the job is launched. Submitting a file whose previous import failed or was stopped resumes
     * that import; submitting a file that was already imported completely is refused.
     */
    public ImportStatus start(String requestedFile) {
        Path file = resolveInsideInputDir(requestedFile);
        try {
            ImportFileValidator.checkReadableCsv(file);
        } catch (InvalidFileException e) {
            throw new ImportRequestException(Reason.INVALID, e.getMessage(), e);
        }
        JobParameters parameters = new JobParametersBuilder()
                .addString(ImportJobParameters.FILE, file.toString())
                .addString(ImportJobParameters.FILE_SHA256, FileChecksums.sha256(file))
                .toJobParameters();
        try {
            return launched(jobOperator.start(importJob, parameters));
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new ImportRequestException(Reason.CONFLICT,
                    "File %s with this exact content was already imported successfully".formatted(file), e);
        } catch (JobExecutionAlreadyRunningException e) {
            throw new ImportRequestException(Reason.CONFLICT, "An import of %s is already running".formatted(file), e);
        } catch (InvalidJobParametersException e) {
            throw new ImportRequestException(Reason.INVALID, e.getMessage(), e);
        } catch (JobRestartException e) {
            throw new ImportRequestException(Reason.CONFLICT, e.getMessage(), e);
        }
    }

    /** Resumes a FAILED or STOPPED run from its last committed chunk. */
    public ImportStatus restart(long jobExecutionId) {
        JobExecution execution = findExecution(jobExecutionId);
        if (execution.getStatus() == BatchStatus.COMPLETED) {
            throw new ImportRequestException(Reason.CONFLICT, "Import run %d already completed".formatted(jobExecutionId));
        }
        if (lifecycle.isRunningHere(jobExecutionId)) {
            throw new ImportRequestException(Reason.CONFLICT, "Import run %d is still running".formatted(jobExecutionId));
        }
        // Recorded as running or UNKNOWN but not running in this process: left over from a crash or
        // a database outage. See ImportJobLifecycle#markRestartable for why resuming is safe.
        ImportJobLifecycle.markRestartable(jobRepository, execution);
        try {
            return launched(jobOperator.restart(execution));
        } catch (JobRestartException e) {
            throw new ImportRequestException(Reason.CONFLICT, e.getMessage(), e);
        }
    }

    /** Asks a running import to stop after its current chunk. It ends STOPPED and can be restarted. */
    public ImportStatus stop(long jobExecutionId) {
        JobExecution execution = findExecution(jobExecutionId);
        if (!lifecycle.isRunningHere(jobExecutionId) || execution.getStatus() != BatchStatus.STARTED) {
            throw new ImportRequestException(Reason.CONFLICT, "Import run %d is not running (status %s)"
                    .formatted(jobExecutionId, execution.getStatus()));
        }
        ImportJobLifecycle.requestStop(jobRepository, execution);
        return status(jobExecutionId);
    }

    public ImportStatus status(long jobExecutionId) {
        JobExecution execution = findExecution(jobExecutionId);
        long instanceId = execution.getJobInstanceId();

        long read = 0, written = 0, readSkips = 0, processSkips = 0, commits = 0, rollbacks = 0;
        for (StepExecution step : execution.getStepExecutions()) {
            read += step.getReadCount();
            written += step.getWriteCount();
            readSkips += step.getReadSkipCount();
            processSkips += step.getProcessSkipCount();
            commits += step.getCommitCount();
            rollbacks += step.getRollbackCount();
        }

        Map<String, Long> rejected = new LinkedHashMap<>();
        jdbcTemplate.query("""
                SELECT reason_code, count(*) FROM rejected_transaction
                WHERE job_instance_id = ? GROUP BY reason_code ORDER BY reason_code
                """, rs -> {
            rejected.put(rs.getString(1), rs.getLong(2));
        }, instanceId);
        // The writer counts duplicates as written (they reached the database), so subtract them.
        long writtenAllRuns = jobRepository.getJobExecutions(execution.getJobInstance()).stream()
                .flatMap(e -> e.getStepExecutions().stream())
                .mapToLong(StepExecution::getWriteCount)
                .sum();
        long inserted = writtenAllRuns - rejected.getOrDefault("DUPLICATE", 0L);
        long rejectedTotal = rejected.values().stream().mapToLong(Long::longValue).sum();

        String exitDescription = execution.getExitStatus().getExitDescription();
        if (exitDescription != null && exitDescription.length() > MAX_EXIT_DESCRIPTION) {
            exitDescription = exitDescription.substring(0, MAX_EXIT_DESCRIPTION) + "...";
        }
        return new ImportStatus(
                execution.getId(),
                instanceId,
                execution.getJobParameters().getString(ImportJobParameters.FILE),
                execution.getStatus().name(),
                execution.getExitStatus().getExitCode(),
                exitDescription == null || exitDescription.isBlank() ? null : exitDescription,
                execution.getStartTime(),
                execution.getEndTime(),
                new ImportStatus.RunCounts(read, written, readSkips, processSkips, commits, rollbacks),
                new ImportStatus.ImportTotals(inserted, rejected, inserted + rejectedTotal));
    }

    public List<RejectedLine> rejections(long jobExecutionId, int offset, int limit) {
        long instanceId = findExecution(jobExecutionId).getJobInstanceId();
        return jdbcTemplate.query("""
                SELECT line_number, reason_code, reason, raw_line, job_execution_id
                FROM rejected_transaction WHERE job_instance_id = ?
                ORDER BY line_number NULLS LAST, id
                OFFSET ? LIMIT ?
                """, (rs, i) -> new RejectedLine(
                        rs.getObject(1, Long.class), rs.getString(2), rs.getString(3), rs.getString(4), rs.getLong(5)),
                instanceId, offset, limit);
    }

    private ImportStatus launched(JobExecution execution) {
        // With a bounded executor the launch itself can be refused; Spring Batch then marks the run FAILED.
        if (execution.getStatus() == BatchStatus.FAILED
                && String.valueOf(execution.getExitStatus().getExitDescription()).contains("TaskRejectedException")) {
            throw new ImportRequestException(Reason.BUSY,
                    "Too many imports are running; try again later. Re-submitting the same file will resume this run.");
        }
        log.info("Launched import jobExecutionId={} for {}", execution.getId(),
                execution.getJobParameters().getString(ImportJobParameters.FILE));
        return status(execution.getId());
    }

    private JobExecution findExecution(long jobExecutionId) {
        JobExecution execution;
        try {
            execution = jobRepository.getJobExecution(jobExecutionId);
        } catch (EmptyResultDataAccessException e) {
            execution = null;
        }
        if (execution == null) {
            throw new ImportRequestException(Reason.NOT_FOUND, "No import run with id " + jobExecutionId);
        }
        return execution;
    }

    /** Only files inside the configured input directory may be imported (no ../ or symlink escapes). */
    private Path resolveInsideInputDir(String requestedFile) {
        if (requestedFile == null || requestedFile.isBlank()) {
            throw new ImportRequestException(Reason.INVALID, "'file' is required");
        }
        try {
            Path root = inputDir.toRealPath();
            Path candidate = root.resolve(requestedFile).normalize();
            if (!Files.exists(candidate)) {
                throw new ImportRequestException(Reason.INVALID, "File %s does not exist in the input directory"
                        .formatted(requestedFile));
            }
            Path real = candidate.toRealPath();
            if (!real.startsWith(root)) {
                throw new ImportRequestException(Reason.INVALID, "File must be inside the input directory");
            }
            return real;
        } catch (IOException e) {
            throw new ImportRequestException(Reason.INVALID, "Cannot resolve file %s: %s".formatted(requestedFile, e));
        }
    }
}
