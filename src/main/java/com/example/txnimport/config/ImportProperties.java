package com.example.txnimport.config;

import java.nio.file.Path;
import java.time.Duration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.import")
public record ImportProperties(
        @NotNull Path inputDir,
        @Min(1) int chunkSize,
        @Min(0) long skipLimit,
        @Min(1) int maxConcurrentJobs,
        @Min(1) int progressLogIntervalChunks,
        @Valid @NotNull AutoRestart autoRestart,
        boolean recoverStaleExecutionsOnStartup) {

    public record AutoRestart(
            boolean enabled,
            @Min(0) int maxAttempts,
            @NotNull Duration initialBackoff,
            @DecimalMin("1.0") double multiplier) {
    }
}
