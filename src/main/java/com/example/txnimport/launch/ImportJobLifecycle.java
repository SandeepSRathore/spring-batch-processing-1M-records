package com.example.txnimport.launch;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.example.txnimport.config.BatchConfig;
import com.example.txnimport.config.ImportProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskDecorator;
import org.springframework.stereotype.Component;

/**
 * Keeps import runs healthy across failures and process restarts:
 * <ul>
 * <li>restarts a run that failed on a transient error, from its last checkpoint, with backoff;</li>
 * <li>at startup, marks runs left STARTED by a crashed process as FAILED so they can be restarted;</li>
 * <li>on shutdown, asks running jobs to stop at the next chunk boundary so they end STOPPED
 * (restartable) instead of being killed mid-chunk and left STARTED.</li>
 * </ul>
 * It is also the {@link TaskDecorator} of the job executor, to catch the case where a job
 * thread dies without {@link #afterJob} being called (Spring Batch saves the final status
 * before calling listeners, so a database outage at that moment skips them).
 */
@Component
public class ImportJobLifecycle implements JobExecutionListener, SmartLifecycle, TaskDecorator {

    private static final Logger log = LoggerFactory.getLogger(ImportJobLifecycle.class);
    private static final Duration SHUTDOWN_WAIT = Duration.ofSeconds(50);

    private record RunningJob(long jobInstanceId, Thread thread) {
    }

    private final JobRepository jobRepository;
    // Lazy: the JobOperator registers all Job beans, and the Job uses this listener.
    private final ObjectProvider<JobOperator> jobOperator;
    private final ImportProperties.AutoRestart autoRestart;
    private final boolean recoverStaleExecutions;

    /** Executions running in this JVM, by id. Anything else marked STARTED in the database is stale. */
    private final Map<Long, RunningJob> runningHere = new ConcurrentHashMap<>();
    /** Automatic restart attempts per job instance, reset when the instance completes. */
    private final Map<Long, Integer> restartAttempts = new ConcurrentHashMap<>();
    private final ScheduledExecutorService restartScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("import-auto-restart").daemon(true).factory());

    private volatile boolean running;

    public ImportJobLifecycle(JobRepository jobRepository, ObjectProvider<JobOperator> jobOperator,
            ImportProperties properties) {
        this.jobRepository = jobRepository;
        this.jobOperator = jobOperator;
        this.autoRestart = properties.autoRestart();
        this.recoverStaleExecutions = properties.recoverStaleExecutionsOnStartup();
    }

    public boolean isRunningHere(long jobExecutionId) {
        return runningHere.containsKey(jobExecutionId);
    }

    boolean hasRunningJobs() {
        return !runningHere.isEmpty();
    }

    /**
     * Asks a running execution to stop: the step checks the job status at every chunk commit and
     * ends STOPPED, restartable from that commit.
     *
     * <p>Deliberately not {@code JobOperator.stop}: besides flagging the job, it saves a copy of
     * the step execution read moments earlier, and its execution context is written without a
     * version check. If the step commits a chunk in between, the saved reader position goes back
     * and a restart re-reads already committed lines. Updating only the job execution is safe,
     * because the job repository synchronises job execution versions with the running job.
     */
    static void requestStop(JobRepository jobRepository, JobExecution execution) {
        execution.setStatus(BatchStatus.STOPPING);
        jobRepository.update(execution);
    }

    /**
     * Makes a run that is <em>not running in this process</em> restartable again by marking every
     * job or step execution still recorded as running (STARTING/STARTED/STOPPING) or UNKNOWN as
     * FAILED. Callers must have checked {@link #isRunningHere} first.
     *
     * <p>The database can be left in any of these states when the process or the database dies:
     * <ul>
     * <li>process killed: job and step still STARTED;</li>
     * <li>database lost mid-commit/rollback: Spring Batch marks the step (and usually the job)
     * UNKNOWN, meaning "the chunk may or may not have been written";</li>
     * <li>database lost when the step ends but back before the job ends: step still STARTED, job
     * FAILED. Spring Batch refuses to restart while any step is recorded as running.</li>
     * </ul>
     * Resuming is safe in all of them for this job: the chunk's rows, its rejections and the
     * reader position are committed in one transaction on one database, so whatever the database
     * kept is consistent, and inserts are idempotent on transaction_id anyway.
     *
     * @return the execution, updated in place
     */
    static JobExecution markRestartable(JobRepository jobRepository, JobExecution execution) {
        LocalDateTime now = LocalDateTime.now();
        for (StepExecution step : execution.getStepExecutions()) {
            if (step.getStatus().isRunning() || step.getStatus() == BatchStatus.UNKNOWN) {
                log.warn("Import jobExecutionId={}: step {} is recorded as {} but not running; marking it FAILED so "
                        + "it resumes from the last committed checkpoint", execution.getId(), step.getStepName(),
                        step.getStatus());
                step.setStatus(BatchStatus.FAILED);
                if (step.getEndTime() == null) {
                    step.setEndTime(now);
                }
                jobRepository.update(step);
            }
        }
        if (execution.getStatus().isRunning() || execution.getStatus() == BatchStatus.UNKNOWN) {
            log.warn("Import jobExecutionId={} is recorded as {} but not running; marking it FAILED",
                    execution.getId(), execution.getStatus());
            execution.setStatus(BatchStatus.FAILED);
            if (execution.getEndTime() == null) {
                execution.setEndTime(now);
            }
            jobRepository.update(execution);
        }
        return execution;
    }

    private void requestStop(JobExecution execution) {
        requestStop(jobRepository, execution);
    }

    // ---- JobExecutionListener ----------------------------------------------------------------

    @Override
    public void beforeJob(JobExecution jobExecution) {
        runningHere.put(jobExecution.getId(), new RunningJob(jobExecution.getJobInstanceId(), Thread.currentThread()));
        log.info("Import job starting: jobExecutionId={}, jobInstanceId={}, parameters={}",
                jobExecution.getId(), jobExecution.getJobInstanceId(), jobExecution.getJobParameters());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        runningHere.remove(jobExecution.getId());
        long instanceId = jobExecution.getJobInstanceId();
        BatchStatus status = jobExecution.getStatus();
        log.info("Import job finished: jobExecutionId={}, status={}, exitCode={}, duration={}",
                jobExecution.getId(), status, jobExecution.getExitStatus().getExitCode(), duration(jobExecution));

        if (status == BatchStatus.COMPLETED) {
            restartAttempts.remove(instanceId);
            return;
        }
        // UNKNOWN: the database went away so abruptly that even the rollback failed (see markRestartable).
        if (status != BatchStatus.FAILED && status != BatchStatus.UNKNOWN) {
            return;
        }
        // Stack traces are already logged by Spring Batch where the failure happened. The stored exit
        // description is replaced by the cause chain, which is what an operator needs to see first.
        String causes = jobExecution.getAllFailureExceptions().stream()
                .map(ImportJobLifecycle::causeChain)
                .distinct()
                .collect(Collectors.joining(" | "));
        log.error("Import jobExecutionId={} failed: {}", jobExecution.getId(), causes);
        jobExecution.setExitStatus(new ExitStatus(jobExecution.getExitStatus().getExitCode(), causes));
        if (TransientFailures.anyTransient(jobExecution.getAllFailureExceptions())) {
            scheduleRestart(jobExecution.getId(), instanceId);
        }
    }

    // ---- TaskDecorator -----------------------------------------------------------------------

    @Override
    public Runnable decorate(Runnable jobLaunch) {
        return () -> {
            try {
                jobLaunch.run();
            } finally {
                runningHere.entrySet().stream()
                        .filter(entry -> entry.getValue().thread() == Thread.currentThread())
                        .toList()
                        .forEach(entry -> {
                            runningHere.remove(entry.getKey());
                            log.error("Import jobExecutionId={} ended without saving its final status "
                                    + "(job repository unreachable?)", entry.getKey());
                            scheduleRestart(entry.getKey(), entry.getValue().jobInstanceId());
                        });
            }
        };
    }

    // ---- Automatic restart -------------------------------------------------------------------

    private void scheduleRestart(long jobExecutionId, long instanceId) {
        if (!autoRestart.enabled() || !running) {
            return;
        }
        int attempt = restartAttempts.merge(instanceId, 1, Integer::sum);
        if (attempt > autoRestart.maxAttempts()) {
            log.error("Import jobInstanceId={} failed on a transient error but already used {} automatic restarts; "
                    + "restart it manually once the cause is fixed", instanceId, autoRestart.maxAttempts());
            restartAttempts.remove(instanceId);
            return;
        }
        long delayMillis = (long) (autoRestart.initialBackoff().toMillis()
                * Math.pow(autoRestart.multiplier(), attempt - 1));
        log.warn("Import jobExecutionId={} failed on a transient error; automatic restart {}/{} in {} ms",
                jobExecutionId, attempt, autoRestart.maxAttempts(), delayMillis);
        restartScheduler.schedule(() -> restart(jobExecutionId, instanceId), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void restart(long failedExecutionId, long instanceId) {
        if (!running) {
            return;
        }
        try {
            JobExecution failed = jobRepository.getJobExecution(failedExecutionId);
            JobExecution latest = jobRepository.getLastJobExecution(failed.getJobInstance());
            if (latest != null && latest.getId() != failedExecutionId) {
                log.info("Import jobInstanceId={} was already restarted (jobExecutionId={}); skipping automatic restart",
                        instanceId, latest.getId());
                return;
            }
            if (isRunningHere(failedExecutionId)) {
                return;
            }
            // The final status may not have been saved, or saved only partly, if the database was down.
            markRestartable(jobRepository, failed);
            JobExecution restarted = jobOperator.getObject().restart(failed);
            log.info("Automatically restarted jobInstanceId={} as jobExecutionId={}", instanceId, restarted.getId());
        } catch (Exception e) {
            // Most likely the database is still unavailable: try again with the next backoff step.
            log.warn("Automatic restart of jobExecutionId={} failed: {}", failedExecutionId, e.toString());
            scheduleRestart(failedExecutionId, instanceId);
        }
    }

    // ---- Startup recovery --------------------------------------------------------------------

    @EventListener(ApplicationReadyEvent.class)
    void recoverStaleExecutions() {
        if (!recoverStaleExecutions) {
            return;
        }
        for (JobExecution stale : jobRepository.findRunningJobExecutions(BatchConfig.JOB_NAME)) {
            if (isRunningHere(stale.getId())) {
                continue;
            }
            log.warn("Found import jobExecutionId={} still marked {} from a previous process", stale.getId(),
                    stale.getStatus());
            markRestartable(jobRepository, stale);
        }
    }

    // ---- Graceful shutdown -------------------------------------------------------------------

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        restartScheduler.shutdownNow();
        for (Long id : runningHere.keySet()) {
            try {
                log.info("Shutting down: asking import jobExecutionId={} to stop at the next chunk boundary", id);
                requestStop(jobRepository.getJobExecution(id));
            } catch (Exception e) {
                log.warn("Could not request stop of jobExecutionId={}: {}", id, e.toString());
            }
        }
        long deadline = System.nanoTime() + SHUTDOWN_WAIT.toNanos();
        while (!runningHere.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!runningHere.isEmpty()) {
            log.error("Import executions {} did not stop within {}; they will be recovered at next startup",
                    runningHere.keySet(), SHUTDOWN_WAIT);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private static String causeChain(Throwable failure) {
        List<String> parts = new ArrayList<>();
        Set<Throwable> seen = new HashSet<>();
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            parts.add(t.getClass().getSimpleName() + ": " + t.getMessage());
        }
        return String.join(" <- ", parts);
    }

    private static Duration duration(JobExecution execution) {
        if (execution.getStartTime() == null || execution.getEndTime() == null) {
            return Duration.ZERO;
        }
        return Duration.between(execution.getStartTime(), execution.getEndTime());
    }
}
