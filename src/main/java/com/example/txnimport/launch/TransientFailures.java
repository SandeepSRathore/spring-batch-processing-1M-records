package com.example.txnimport.launch;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * Decides whether a failed run is worth restarting automatically: true only for failures
 * that can go away on their own (lost connection, deadlock, database restarting), never for
 * bad input or bugs, which would just fail again.
 */
final class TransientFailures {

    /**
     * SQLState classes: 08 connection exception, 40 transaction rollback (deadlock,
     * serialization failure), 53 insufficient resources, 57P01-03 server shutting down.
     */
    private static final Set<String> TRANSIENT_SQL_STATE_PREFIXES = Set.of("08", "40", "53", "57P01", "57P02", "57P03");

    private TransientFailures() {
    }

    static boolean anyTransient(Collection<Throwable> failures) {
        return !failures.isEmpty() && failures.stream().anyMatch(TransientFailures::isTransient);
    }

    static boolean isTransient(Throwable failure) {
        Set<Throwable> seen = new HashSet<>();
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof TransientDataAccessException
                    || t instanceof RecoverableDataAccessException
                    || t instanceof DataAccessResourceFailureException
                    || t instanceof CannotCreateTransactionException
                    || t instanceof SQLTransientException
                    || t instanceof SQLRecoverableException) {
                return true;
            }
            if (t instanceof SQLException sql && sql.getSQLState() != null
                    && TRANSIENT_SQL_STATE_PREFIXES.stream().anyMatch(sql.getSQLState()::startsWith)) {
                return true;
            }
        }
        return false;
    }
}
