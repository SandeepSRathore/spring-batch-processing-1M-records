-- Successfully imported transactions. transaction_id is the natural key: inserts use
-- ON CONFLICT DO NOTHING, so a re-run or overlapping file can never create duplicates.
CREATE TABLE customer_transaction (
    transaction_id   VARCHAR(40)    PRIMARY KEY,
    customer_id      VARCHAR(20)    NOT NULL,
    amount           NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    currency         CHAR(3)        NOT NULL,
    type             VARCHAR(6)     NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    occurred_at      TIMESTAMPTZ    NOT NULL,
    merchant         VARCHAR(100),
    status           VARCHAR(10)    NOT NULL CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED')),
    source_file      TEXT           NOT NULL,
    source_line      BIGINT         NOT NULL,
    job_instance_id  BIGINT         NOT NULL,
    imported_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX idx_customer_transaction_customer ON customer_transaction (customer_id, occurred_at);

-- Every input line that did not end up in customer_transaction, with the reason.
-- Rows are written in the same transaction as the chunk they belong to.
CREATE TABLE rejected_transaction (
    id               BIGSERIAL   PRIMARY KEY,
    job_instance_id  BIGINT      NOT NULL,
    job_execution_id BIGINT      NOT NULL,
    source_file      TEXT        NOT NULL,
    line_number      BIGINT,
    raw_line         TEXT,
    reason_code      VARCHAR(20) NOT NULL CHECK (reason_code IN ('PARSE_ERROR', 'VALIDATION_ERROR', 'DUPLICATE')),
    reason           TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rejected_transaction_instance ON rejected_transaction (job_instance_id, line_number);
