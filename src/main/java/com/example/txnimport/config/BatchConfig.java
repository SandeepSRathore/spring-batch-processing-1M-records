package com.example.txnimport.config;

import java.time.Clock;
import java.util.Set;

import com.example.txnimport.batch.ImportFileValidator;
import com.example.txnimport.batch.ImportJobParameters;
import com.example.txnimport.batch.ImportProgressListener;
import com.example.txnimport.batch.InvalidTransactionException;
import com.example.txnimport.batch.RejectionRecorder;
import com.example.txnimport.batch.RejectionSkipListener;
import com.example.txnimport.batch.Transaction;
import com.example.txnimport.batch.TransactionCsvFormat;
import com.example.txnimport.batch.TransactionCsvRow;
import com.example.txnimport.batch.TransactionLineMapper;
import com.example.txnimport.batch.TransactionValidatingProcessor;
import com.example.txnimport.batch.TransactionWriter;
import com.example.txnimport.launch.ImportJobLifecycle;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingExceptionHierarchySkipPolicy;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.batch.autoconfigure.BatchTaskExecutor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ImportProperties.class)
public class BatchConfig {

    public static final String JOB_NAME = "transactionImportJob";
    public static final String STEP_NAME = "importTransactions";

    @Bean
    Job transactionImportJob(JobRepository jobRepository, Step importTransactionsStep, ImportJobLifecycle lifecycle) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .validator(new ImportFileValidator())
                .listener(lifecycle)
                .start(importTransactionsStep)
                .build();
    }

    /**
     * Single-threaded on purpose: {@link FlatFileItemReader} is not thread-safe and its
     * restart position is only meaningful when one thread reads the file in order.
     */
    @Bean
    Step importTransactionsStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            FlatFileItemReader<TransactionCsvRow> transactionReader, JdbcTemplate jdbcTemplate,
            ImportProperties properties, Clock clock) {
        var recorder = new RejectionRecorder(jdbcTemplate);
        var progress = new ImportProgressListener(properties.progressLogIntervalChunks());
        // Only bad input is skippable. Write and infrastructure failures fail the step, leaving it restartable.
        var skipPolicy = new LimitCheckingExceptionHierarchySkipPolicy(
                Set.of(FlatFileParseException.class, InvalidTransactionException.class), properties.skipLimit());

        return new StepBuilder(STEP_NAME, jobRepository)
                .<TransactionCsvRow, Transaction>chunk(properties.chunkSize())
                .transactionManager(transactionManager)
                .reader(transactionReader)
                .processor(new TransactionValidatingProcessor(clock))
                .writer(new TransactionWriter(jdbcTemplate, recorder))
                .faultTolerant()
                .skipPolicy(skipPolicy)
                .skipListener(new RejectionSkipListener(recorder))
                .listener(progress)
                .build();
    }

    @Bean
    @StepScope
    FlatFileItemReader<TransactionCsvRow> transactionReader(
            @Value("#{jobParameters['" + ImportJobParameters.FILE + "']}") String file) {
        return new FlatFileItemReaderBuilder<TransactionCsvRow>()
                // Prefix for the restart position stored in the step execution context.
                .name("transactionReader")
                .resource(new FileSystemResource(file))
                .encoding("UTF-8")
                .strict(true)
                .linesToSkip(1)
                // Re-checked on every (re)start, in case the file was swapped for another one.
                .skippedLinesCallback(TransactionCsvFormat::requireHeader)
                // One physical line = one record. The default policy lets a stray quote swallow every
                // following line up to the end of the file into a single "record".
                .recordSeparatorPolicy(new SimpleRecordSeparatorPolicy())
                // No comment prefixes: a line starting with '#' must be reported, not silently dropped.
                .comments()
                .lineMapper(new TransactionLineMapper())
                .build();
    }

    /** Runs jobs off the request thread; the REST call returns as soon as the job is launched. */
    @Bean
    @BatchTaskExecutor
    TaskExecutor batchTaskExecutor(ImportProperties properties, ObjectProvider<ImportJobLifecycle> lifecycle) {
        var executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("import-job-");
        // Resolved lazily: Spring Batch's configuration needs this executor before the job repository exists.
        executor.setTaskDecorator(jobLaunch -> lifecycle.getObject().decorate(jobLaunch));
        executor.setCorePoolSize(properties.maxConcurrentJobs());
        executor.setMaxPoolSize(properties.maxConcurrentJobs());
        // No queue: if every worker is busy the launch is refused instead of silently waiting.
        executor.setQueueCapacity(0);
        return executor;
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
