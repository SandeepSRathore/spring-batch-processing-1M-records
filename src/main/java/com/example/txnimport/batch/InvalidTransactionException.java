package com.example.txnimport.batch;

import java.util.List;

/** A row that parsed as CSV but breaks one or more business rules. Skippable. */
public class InvalidTransactionException extends RuntimeException {

    private final List<String> violations;

    public InvalidTransactionException(List<String> violations) {
        super(String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
