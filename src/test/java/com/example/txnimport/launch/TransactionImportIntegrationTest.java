package com.example.txnimport.launch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.example.txnimport.batch.TransactionCsvFormat;
import com.example.txnimport.config.BatchConfig;
import com.example.txnimport.launch.ImportRequestException.Reason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "app.import.chunk-size=3",
        "app.import.skip-limit=7",
        "app.import.progress-log-interval-chunks=1",
        "app.import.auto-restart.initial-backoff=100ms",
        "app.import.auto-restart.max-attempts=2",
})
class TransactionImportIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    static final Path INPUT_DIR = createTempDir();

    @DynamicPropertySource
    static void inputDir(DynamicPropertyRegistry registry) {
        registry.add("app.import.input-dir", INPUT_DIR::toString);
    }

    /**
     * 15 data lines (header is line 1). Expected: 6 inserted, 5 parse errors, 2 validation
     * errors, 2 duplicates. Line 15 has a stray quote and must not swallow line 16. This is
     * exactly 7 skips, the configured skip limit for this test class.
     */
    static final String FIXTURE = TransactionCsvFormat.HEADER + "\n" + """
            TXN-0000000001,CUST-000001,10.00,USD,DEBIT,2026-01-01T00:00:00Z,Shop,COMPLETED
            TXN-0000000002,CUST-000002,20.00,EUR,CREDIT,2026-01-02T00:00:00Z,,PENDING
            TXN-0000000003,CUST-000003,abc,USD,DEBIT,2026-01-03T00:00:00Z,Shop,COMPLETED
            TXN-0000000004,CUST-000004,40.00,USD,DEBIT

            # exported by legacy system
            TXN-0000000001,CUST-000001,10.00,USD,DEBIT,2026-01-01T00:00:00Z,Shop,COMPLETED
            TXN-0000000005,CUST-000005,50.00,GBP,CREDIT,2026-01-05T00:00:00Z,"Acme, Inc.",REVERSED
            TXN-0000000006,CUST-000006,60.00,XYZ,DEBIT,2026-01-06T00:00:00Z,Shop,FAILED
            TXN-0000000007,CUST-000007,70.00,JPY,DEBIT,2026-01-07T00:00:00Z,Shop,PENDING,extra
            TXN-0000000008,CUST-000008,80.00,INR,DEBIT,2026-01-08T00:00:00Z,Shop,COMPLETED
            TXN-0000000009,CUST-000009,90.00,AUD,CREDIT,2026-01-09T00:00:00Z,,COMPLETED
            TXN-0000000008,CUST-000008,80.00,INR,DEBIT,2026-01-08T00:00:00Z,Shop,COMPLETED
            TXN-0000000010,CUST-000010,100.00,CAD,DEBIT,2026-01-10T00:00:00Z,"Unbalanced ,COMPLETED
            TXN-0000000011,CUST-000011,110.00,SGD,CREDIT,2026-01-11T00:00:00Z,,PENDING
            """;

    static final Map<String, Long> FIXTURE_REJECTIONS = Map.of("PARSE_ERROR", 5L, "VALIDATION_ERROR", 2L, "DUPLICATE", 2L);
    static final List<String> FIXTURE_INSERTED_IDS = List.of("TXN-0000000001", "TXN-0000000002", "TXN-0000000005",
            "TXN-0000000008", "TXN-0000000009", "TXN-0000000011");

    @Autowired ImportService importService;
    @Autowired ImportJobLifecycle lifecycle;
    @Autowired JobRepository jobRepository;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mockMvc;

    @AfterEach
    void waitForBackgroundJobs() {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (lifecycle.hasRunningJobs() && System.nanoTime() < deadline) {
            sleep();
        }
    }

    @BeforeEach
    void resetDatabase() {
        jdbc.execute("""
                TRUNCATE customer_transaction, rejected_transaction,
                    batch_step_execution_context, batch_job_execution_context, batch_step_execution,
                    batch_job_execution_params, batch_job_execution, batch_job_instance
                """);
        installFaultInjection();
    }

    @Test
    void importsValidRowsAndAccountsForEveryRejectedLine() {
        ImportStatus result = awaitEnd(importService.start(write("fixture.csv", FIXTURE)).jobExecutionId());

        assertThat(result.status()).isEqualTo("COMPLETED");
        assertThat(insertedIds()).containsExactlyElementsOf(FIXTURE_INSERTED_IDS);
        assertThat(result.importTotals().inserted()).isEqualTo(6);
        assertThat(result.importTotals().rejected()).isEqualTo(FIXTURE_REJECTIONS);
        assertThat(result.importTotals().linesAccountedFor()).isEqualTo(15);

        Map<Long, String> reasonByLine = importService.rejections(result.jobExecutionId(), 0, 100).stream()
                .collect(Collectors.toMap(RejectedLine::lineNumber, RejectedLine::reasonCode));
        assertThat(reasonByLine).containsExactlyInAnyOrderEntriesOf(Map.of(
                4L, "VALIDATION_ERROR", 5L, "PARSE_ERROR", 6L, "PARSE_ERROR", 7L, "PARSE_ERROR", 8L, "DUPLICATE",
                10L, "VALIDATION_ERROR", 11L, "PARSE_ERROR", 14L, "DUPLICATE", 15L, "PARSE_ERROR"));

        Map<String, Object> stored = jdbc.queryForMap(
                "SELECT merchant, currency, amount FROM customer_transaction WHERE transaction_id = 'TXN-0000000005'");
        assertThat(stored.get("merchant")).isEqualTo("Acme, Inc.");
    }

    @Test
    void sameFileIsNotImportedTwice() {
        String file = write("once.csv", FIXTURE);
        awaitEnd(importService.start(file).jobExecutionId());

        assertThatThrownBy(() -> importService.start(file))
                .isInstanceOf(ImportRequestException.class)
                .extracting(e -> ((ImportRequestException) e).reason()).isEqualTo(Reason.CONFLICT);
        assertThat(count("customer_transaction")).isEqualTo(6);
    }

    @Test
    void overlappingFileRecordsDuplicatesInsteadOfInsertingThemAgain() {
        awaitEnd(importService.start(write("first.csv", FIXTURE)).jobExecutionId());
        String overlapping = TransactionCsvFormat.HEADER + "\n" + """
                TXN-0000000001,CUST-000001,10.00,USD,DEBIT,2026-01-01T00:00:00Z,Shop,COMPLETED
                TXN-0000000099,CUST-000099,99.00,USD,CREDIT,2026-01-01T00:00:00Z,,COMPLETED
                """;

        ImportStatus result = awaitEnd(importService.start(write("second.csv", overlapping)).jobExecutionId());

        assertThat(result.status()).isEqualTo("COMPLETED");
        assertThat(result.importTotals().inserted()).isEqualTo(1);
        assertThat(result.importTotals().rejected()).isEqualTo(Map.of("DUPLICATE", 1L));
        assertThat(count("customer_transaction")).isEqualTo(7);
    }

    @Test
    void failedRunResumesFromLastCommittedChunkWithoutDuplicatesOrLostRejections() {
        // Chunks are 3 lines; the fault hits line 13, in the 4th chunk (lines 11-13).
        injectFault("TXN-0000000009", "P0001", false);
        ImportStatus failed = awaitEnd(importService.start(write("restart.csv", FIXTURE)).jobExecutionId());

        assertThat(failed.status()).isEqualTo("FAILED");
        assertThat(insertedIds()).containsExactly("TXN-0000000001", "TXN-0000000002", "TXN-0000000005");
        assertThat(count("rejected_transaction")).isEqualTo(6); // lines 4-8 and 10; line 11's rejection rolled back

        jdbc.update("DELETE FROM test_fault");
        ImportStatus restarted = awaitEnd(importService.restart(failed.jobExecutionId()).jobExecutionId());

        assertThat(restarted.status()).isEqualTo("COMPLETED");
        assertThat(restarted.jobInstanceId()).isEqualTo(failed.jobInstanceId());
        assertThat(insertedIds()).containsExactlyElementsOf(FIXTURE_INSERTED_IDS);
        assertThat(restarted.importTotals().inserted()).isEqualTo(6);
        assertThat(restarted.importTotals().rejected()).isEqualTo(FIXTURE_REJECTIONS);
        assertThat(restarted.importTotals().linesAccountedFor()).isEqualTo(15);
    }

    @Test
    void runWithUnknownTransactionOutcomeCanBeResumed() {
        // UNKNOWN is what Spring Batch records when the connection dies during commit/rollback,
        // which cannot be provoked reliably in a test: reproduce the resulting state instead.
        injectFault("TXN-0000000009", "P0001", false);
        ImportStatus failed = awaitEnd(importService.start(write("unknown.csv", FIXTURE)).jobExecutionId());
        jdbc.update("UPDATE batch_job_execution SET status = 'UNKNOWN' WHERE job_execution_id = ?", failed.jobExecutionId());
        jdbc.update("UPDATE batch_step_execution SET status = 'UNKNOWN' WHERE job_execution_id = ?", failed.jobExecutionId());
        jdbc.update("DELETE FROM test_fault");

        ImportStatus resumed = awaitEnd(importService.restart(failed.jobExecutionId()).jobExecutionId());

        assertThat(resumed.status()).isEqualTo("COMPLETED");
        assertThat(insertedIds()).containsExactlyElementsOf(FIXTURE_INSERTED_IDS);
        assertThat(resumed.importTotals().rejected()).isEqualTo(FIXTURE_REJECTIONS);
    }

    @Test
    void runWhoseStepStatusWasNeverSavedCanBeResumed() {
        // State left by a live database outage: the database was down when the step ended (step
        // still STARTED) but back in time for the job's final status (FAILED) to be saved.
        injectFault("TXN-0000000009", "P0001", false);
        ImportStatus failed = awaitEnd(importService.start(write("partial.csv", FIXTURE)).jobExecutionId());
        jdbc.update("UPDATE batch_step_execution SET status = 'STARTED', end_time = NULL WHERE job_execution_id = ?",
                failed.jobExecutionId());
        jdbc.update("DELETE FROM test_fault");

        ImportStatus resumed = awaitEnd(importService.restart(failed.jobExecutionId()).jobExecutionId());

        assertThat(resumed.status()).isEqualTo("COMPLETED");
        assertThat(insertedIds()).containsExactlyElementsOf(FIXTURE_INSERTED_IDS);
        assertThat(resumed.importTotals().rejected()).isEqualTo(FIXTURE_REJECTIONS);
    }

    @Test
    void transientDatabaseErrorIsRestartedAutomatically() {
        injectFault("TXN-0000000009", "40001", true); // serialization failure, happens once

        ImportStatus first = awaitEnd(importService.start(write("transient.csv", FIXTURE)).jobExecutionId());
        assertThat(first.status()).isEqualTo("FAILED");

        JobExecution last = awaitLastExecution(first, e -> e.getStatus() == BatchStatus.COMPLETED);
        assertThat(last.getId()).isNotEqualTo(first.jobExecutionId());
        assertThat(insertedIds()).containsExactlyElementsOf(FIXTURE_INSERTED_IDS);
        assertThat(importService.status(last.getId()).importTotals().rejected()).isEqualTo(FIXTURE_REJECTIONS);
    }

    @Test
    void automaticRestartsGiveUpAfterMaxAttempts() throws InterruptedException {
        injectFault("TXN-0000000009", "40001", false); // keeps failing

        ImportStatus first = awaitEnd(importService.start(write("flaky.csv", FIXTURE)).jobExecutionId());
        JobInstance instance = jobRepository.getJobInstance(first.jobInstanceId());

        awaitLastExecution(first, e -> jobRepository.getJobExecutions(instance).size() == 3 && !e.isRunning());
        Thread.sleep(1000); // longer than the next backoff would be
        List<JobExecution> executions = jobRepository.getJobExecutions(instance);
        assertThat(executions).hasSize(3).allMatch(e -> e.getStatus() == BatchStatus.FAILED);
    }

    @Test
    void permanentErrorIsNotRestartedAutomatically() throws InterruptedException {
        injectFault("TXN-0000000009", "P0001", false);

        ImportStatus first = awaitEnd(importService.start(write("permanent.csv", FIXTURE)).jobExecutionId());
        Thread.sleep(500);

        assertThat(jobRepository.getJobExecutions(jobRepository.getJobInstance(first.jobInstanceId()))).hasSize(1);
    }

    @Test
    void tooManyBadRowsFailTheJob() {
        String eightBadLines = TransactionCsvFormat.HEADER + "\n" + "garbage\n".repeat(8);

        ImportStatus result = awaitEnd(importService.start(write("bad.csv", eightBadLines)).jobExecutionId());

        assertThat(result.status()).isEqualTo("FAILED");
        assertThat(result.exitDescription()).contains("SkipLimitExceededException");
    }

    @Test
    void stoppedRunEndsStoppedAndResumes() {
        String rows = IntStream.rangeClosed(1, 150)
                .mapToObj(i -> "TXN-%010d,CUST-000001,1.00,USD,CREDIT,2026-01-01T00:00:00Z,,COMPLETED".formatted(i))
                .collect(Collectors.joining("\n", TransactionCsvFormat.HEADER + "\n", "\n"));
        jdbc.update("INSERT INTO test_slow VALUES (20)"); // 20 ms per row: ~3 s in total
        long id = importService.start(write("stop.csv", rows)).jobExecutionId();
        awaitStatus(id, s -> s.thisRun().commits() > 0);

        importService.stop(id);
        ImportStatus stopped = awaitEnd(id);

        assertThat(stopped.status()).isEqualTo("STOPPED");
        assertThat(count("customer_transaction")).isBetween(1L, 149L);

        jdbc.update("DELETE FROM test_slow");
        ImportStatus resumed = awaitEnd(importService.restart(id).jobExecutionId());

        assertThat(resumed.status()).isEqualTo("COMPLETED");
        assertThat(count("customer_transaction")).isEqualTo(150);
        assertThat(count("rejected_transaction")).isZero();
    }

    @Test
    void restartIsRefusedIfTheFileChangedSinceTheFailedRun() {
        injectFault("TXN-0000000009", "P0001", false);
        String file = write("changed.csv", FIXTURE);
        ImportStatus failed = awaitEnd(importService.start(file).jobExecutionId());
        write("changed.csv", FIXTURE + "TXN-0000000012,CUST-000012,1.00,USD,CREDIT,2026-01-01T00:00:00Z,,PENDING\n");

        assertThatThrownBy(() -> importService.restart(failed.jobExecutionId()))
                .isInstanceOf(ImportRequestException.class)
                .hasMessageContaining("changed since this import was started");
    }

    @Test
    void executionLeftRunningByACrashedProcessIsRecoveredAndRestartable() {
        String file = write("crashed.csv", FIXTURE);
        var parameters = new JobParametersBuilder()
                .addString("file", INPUT_DIR.resolve(file).toAbsolutePath().toString())
                .addString("fileSha256", com.example.txnimport.batch.FileChecksums.sha256(INPUT_DIR.resolve(file)))
                .toJobParameters();
        JobInstance instance = jobRepository.createJobInstance(BatchConfig.JOB_NAME, parameters);
        JobExecution crashed = jobRepository.createJobExecution(instance, parameters, new ExecutionContext());
        crashed.setStatus(BatchStatus.STARTED);
        crashed.setStartTime(LocalDateTime.now());
        jobRepository.update(crashed);

        lifecycle.recoverStaleExecutions();

        assertThat(jobRepository.getJobExecution(crashed.getId()).getStatus()).isEqualTo(BatchStatus.FAILED);
        ImportStatus resumed = awaitEnd(importService.restart(crashed.getId()).jobExecutionId());
        assertThat(resumed.status()).isEqualTo("COMPLETED");
        assertThat(resumed.importTotals().inserted()).isEqualTo(6);
    }

    @Test
    void apiValidatesRequestsAndReportsProgress() throws Exception {
        write("api.csv", FIXTURE);
        write("wrong-header.csv", "id,amount\n1,2\n");

        mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON).content("{\"file\":\"../pom.xml\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON).content("{\"file\":\"missing.csv\"}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON).content("{\"file\":\"wrong-header.csv\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("Unexpected CSV header")));
        mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/imports/999999")).andExpect(status().isNotFound());

        String location = mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"file\":\"api.csv\"}"))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andReturn().getResponse().getHeader("Location");
        long id = Long.parseLong(location.substring(location.lastIndexOf('/') + 1));
        awaitEnd(id);

        mockMvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.importTotals.inserted").value(6))
                .andExpect(jsonPath("$.importTotals.linesAccountedFor").value(15));
        mockMvc.perform(get(location + "/rejections").param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].lineNumber").value(4));
        mockMvc.perform(post("/api/imports").contentType(MediaType.APPLICATION_JSON).content("{\"file\":\"api.csv\"}"))
                .andExpect(status().isConflict());
    }

    // ---- helpers ----------------------------------------------------------------------------------

    /**
     * Spring Batch saves the final status before calling job listeners, which then save the
     * concise exit description: wait for the job thread too, not only for the status.
     */
    private ImportStatus awaitEnd(long jobExecutionId) {
        awaitStatus(jobExecutionId, s -> !BatchStatus.valueOf(s.status()).isRunning()
                && !lifecycle.isRunningHere(jobExecutionId));
        return importService.status(jobExecutionId);
    }

    private ImportStatus awaitStatus(long jobExecutionId, Predicate<ImportStatus> condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            ImportStatus status = importService.status(jobExecutionId);
            if (condition.test(status)) {
                return status;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for import " + jobExecutionId + "; last status " + status);
            }
            sleep();
        }
    }

    private JobExecution awaitLastExecution(ImportStatus first, Predicate<JobExecution> condition) {
        JobInstance instance = jobRepository.getJobInstance(first.jobInstanceId());
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            JobExecution last = jobRepository.getLastJobExecution(instance);
            if (last != null && condition.test(last)) {
                return last;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out; executions: " + jobRepository.getJobExecutions(instance));
            }
            sleep();
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private List<String> insertedIds() {
        return jdbc.queryForList("SELECT transaction_id FROM customer_transaction ORDER BY transaction_id", String.class);
    }

    private long count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
    }

    private static String write(String name, String content) {
        try {
            Files.writeString(INPUT_DIR.resolve(name), content, StandardCharsets.UTF_8);
            return name;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("txn-import-test");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Test-only trigger that makes inserting chosen transaction ids fail with a chosen SQLState.
     * One-shot faults use a sequence because sequence increments survive the rollback.
     */
    private void installFaultInjection() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS test_fault (transaction_id TEXT PRIMARY KEY, sql_state TEXT NOT NULL,
                    one_shot BOOLEAN NOT NULL);
                CREATE TABLE IF NOT EXISTS test_slow (millis INT NOT NULL);
                CREATE SEQUENCE IF NOT EXISTS test_fault_hits;
                CREATE OR REPLACE FUNCTION test_fault_fn() RETURNS trigger AS $$
                DECLARE f test_fault%ROWTYPE;
                BEGIN
                    PERFORM pg_sleep(millis / 1000.0) FROM test_slow;
                    SELECT * INTO f FROM test_fault WHERE transaction_id = NEW.transaction_id;
                    IF FOUND AND (NOT f.one_shot OR nextval('test_fault_hits') = 1) THEN
                        RAISE EXCEPTION 'injected fault for %', NEW.transaction_id USING ERRCODE = f.sql_state;
                    END IF;
                    RETURN NEW;
                END $$ LANGUAGE plpgsql;
                CREATE OR REPLACE TRIGGER test_fault_trg BEFORE INSERT ON customer_transaction
                    FOR EACH ROW EXECUTE FUNCTION test_fault_fn();
                TRUNCATE test_fault, test_slow;
                ALTER SEQUENCE test_fault_hits RESTART;
                """);
    }

    private void injectFault(String transactionId, String sqlState, boolean oneShot) {
        jdbc.update("INSERT INTO test_fault VALUES (?, ?, ?)", transactionId, sqlState, oneShot);
    }
}
