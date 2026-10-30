package com.example.txnimport.api;

import java.net.URI;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import com.example.txnimport.launch.ImportService;
import com.example.txnimport.launch.ImportStatus;
import com.example.txnimport.launch.RejectedLine;

import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/api/imports")
public class ImportController {

    public record StartImportRequest(@NotBlank String file) {
    }

    private final ImportService importService;

    public ImportController(ImportService importService) {
        this.importService = importService;
    }

    /** Launches an import in the background. Poll the returned Location for progress. */
    @PostMapping
    public ResponseEntity<ImportStatus> start(@Valid @RequestBody StartImportRequest request) {
        ImportStatus status = importService.start(request.file());
        return ResponseEntity.accepted().location(URI.create("/api/imports/" + status.jobExecutionId())).body(status);
    }

    @GetMapping("/{jobExecutionId}")
    public ImportStatus status(@PathVariable long jobExecutionId) {
        return importService.status(jobExecutionId);
    }

    @PostMapping("/{jobExecutionId}/restart")
    public ResponseEntity<ImportStatus> restart(@PathVariable long jobExecutionId) {
        ImportStatus status = importService.restart(jobExecutionId);
        return ResponseEntity.accepted().location(URI.create("/api/imports/" + status.jobExecutionId())).body(status);
    }

    @PostMapping("/{jobExecutionId}/stop")
    public ResponseEntity<ImportStatus> stop(@PathVariable long jobExecutionId) {
        return ResponseEntity.accepted().body(importService.stop(jobExecutionId));
    }

    @GetMapping("/{jobExecutionId}/rejections")
    public List<RejectedLine> rejections(@PathVariable long jobExecutionId,
            @RequestParam(defaultValue = "0") @Min(0) int offset,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        return importService.rejections(jobExecutionId, offset, limit);
    }
}
