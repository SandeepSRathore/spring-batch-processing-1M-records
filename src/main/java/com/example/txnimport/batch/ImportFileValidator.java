package com.example.txnimport.batch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersValidator;

/**
 * Checks the file before any step runs, on first start and on every restart. Failing here
 * is much better than skipping a million rows one by one, and the checksum check stops a
 * restart from resuming at line N of a file that has since been replaced.
 */
public class ImportFileValidator implements JobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws InvalidJobParametersException {
        String file = parameters.getString(ImportJobParameters.FILE);
        String expectedSha256 = parameters.getString(ImportJobParameters.FILE_SHA256);
        if (file == null || expectedSha256 == null) {
            throw new InvalidJobParametersException("Job parameters '%s' and '%s' are required"
                    .formatted(ImportJobParameters.FILE, ImportJobParameters.FILE_SHA256));
        }
        try {
            checkReadableCsv(Path.of(file));
            String actualSha256 = FileChecksums.sha256(Path.of(file));
            if (!actualSha256.equals(expectedSha256)) {
                throw new InvalidFileException("File %s changed since this import was started (sha256 %s, expected %s)"
                        .formatted(file, actualSha256, expectedSha256));
            }
        } catch (InvalidFileException | UncheckedIOException e) {
            throw new InvalidJobParametersException(e.getMessage());
        }
    }

    /** Shared with the API so that obviously broken files are rejected before a job is created. */
    public static void checkReadableCsv(Path file) {
        if (!Files.isRegularFile(file)) {
            throw new InvalidFileException("File %s does not exist or is not a regular file".formatted(file));
        }
        if (!Files.isReadable(file)) {
            throw new InvalidFileException("File %s is not readable".formatted(file));
        }
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new InvalidFileException("File %s is empty".formatted(file));
            }
            TransactionCsvFormat.requireHeader(header);
        } catch (IOException e) {
            throw new InvalidFileException("Could not read header of %s: %s".formatted(file, e.getMessage()), e);
        }
    }
}
