
-- DIMENSION: customers (slow-changing)
CREATE TABLE customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    full_name     VARCHAR(200) NOT NULL,
    email         VARCHAR(200) UNIQUE NOT NULL,
    phone         VARCHAR(20),
    country_code  CHAR(2) NOT NULL,
    kyc_status    VARCHAR(20) NOT NULL DEFAULT 'pending'
                  CHECK (kyc_status IN ('pending','verified','rejected','expired')),
    risk_score    SMALLINT NOT NULL DEFAULT 50
                  CHECK (risk_score BETWEEN 0 AND 100),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- SEMI-DIMENSION: accounts (balance update liên tục)
CREATE TABLE accounts (
    account_id      BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES customers(customer_id),
    account_number  VARCHAR(20) UNIQUE NOT NULL,
    account_type    VARCHAR(20) NOT NULL
                    CHECK (account_type IN ('checking','savings','credit','investment')),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    balance         NUMERIC(19,4) NOT NULL DEFAULT 0,
    status          VARCHAR(20) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','frozen','closed','suspended')),
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX idx_accounts_updated_at  ON accounts(updated_at);
ALTER TABLE accounts REPLICA IDENTITY FULL;


-- FACT: transactions (append-only, high volume)

CREATE TABLE transactions (
    transaction_id     BIGSERIAL PRIMARY KEY,
    account_id         BIGINT NOT NULL REFERENCES accounts(account_id),
    transaction_type   VARCHAR(20) NOT NULL
                       CHECK (transaction_type IN
                       ('deposit','withdrawal','fee','interest','transfer_in','transfer_out')),
    amount             NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    balance_after      NUMERIC(19,4) NOT NULL,
    currency           CHAR(3) NOT NULL,
    merchant_name      VARCHAR(200),
    merchant_category  VARCHAR(50),
    description        TEXT,
    status             VARCHAR(20) NOT NULL DEFAULT 'completed'
                       CHECK (status IN ('pending','completed','failed','reversed')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    posted_at          TIMESTAMPTZ
);
CREATE INDEX idx_tx_account_id ON transactions(account_id);
CREATE INDEX idx_tx_created_at ON transactions(created_at);

-- FACT: transfers 
CREATE TABLE transfers (
    transfer_id      BIGSERIAL PRIMARY KEY,
    from_account_id  BIGINT NOT NULL REFERENCES accounts(account_id),
    to_account_id    BIGINT NOT NULL REFERENCES accounts(account_id),
    amount           NUMERIC(19,4) NOT NULL CHECK (amount > 0),
    currency         CHAR(3) NOT NULL,
    status           VARCHAR(20) NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','processing','completed','failed','cancelled')),
    reference_code   VARCHAR(40) UNIQUE NOT NULL,
    failure_reason   TEXT,
    initiated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (from_account_id <> to_account_id)
);
CREATE INDEX idx_transfers_from       ON transfers(from_account_id);
CREATE INDEX idx_transfers_to         ON transfers(to_account_id);
CREATE INDEX idx_transfers_status     ON transfers(status);
CREATE INDEX idx_transfers_updated_at ON transfers(updated_at);
ALTER TABLE transfers REPLICA IDENTITY FULL;