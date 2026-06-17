
INSERT INTO customers (full_name, email, phone, country_code, kyc_status, risk_score)
SELECT
    'Customer ' || LPAD(g.i::text, 4, '0'),
    'customer' || g.i || '@example.com',
    '+' || (10 + (g.i % 90))::text || LPAD((1000000 + g.i * 7)::text, 7, '0'),
    (ARRAY['US','US','US','VN','VN','GB','DE','FR','SG','JP','AU','CA','IN','BR'])
        [1 + (g.i % 14)],
    -- KYC: phần lớn verified, ít pending/rejected (mô phỏng thật)
    CASE
        WHEN g.i % 25 = 0 THEN 'rejected'
        WHEN g.i % 15 = 0 THEN 'pending'
        WHEN g.i % 40 = 0 THEN 'expired'
        ELSE 'verified'
    END,
    -- Risk score: phần lớn 30-70, vài outlier
    CASE
        WHEN g.i % 20 = 0 THEN 85 + (g.i % 15)  -- high risk
        WHEN g.i % 17 = 0 THEN 10 + (g.i % 15)  -- low risk
        ELSE 40 + (g.i % 30)                    -- normal
    END
FROM generate_series(1, 100) AS g(i);

-- Accounts: mỗi customer có 1-3 accounts với type/balance đa dạng
-- CROSS JOIN LATERAL + generate_series: kỹ thuật sinh "n bản ghi con
-- cho mỗi bản ghi cha" rất hữu ích trong test data generation.
INSERT INTO accounts (customer_id, account_number, account_type, currency, balance, status)
SELECT
    c.customer_id,
    -- account_number unique: ghép customer_id + sequence number
    'ACC' || LPAD((c.customer_id * 10 + n.num)::text, 12, '0'),
    -- Mỗi customer có account đa dạng type
    (ARRAY['checking','savings','credit','investment'])[1 + ((c.customer_id + n.num) % 4)],
    -- Currency: phần lớn USD, một số VND/EUR/GBP
    CASE
        WHEN c.country_code = 'VN' THEN 'VND'
        WHEN c.country_code IN ('DE','FR') THEN 'EUR'
        WHEN c.country_code = 'GB' THEN 'GBP'
        WHEN c.country_code = 'JP' THEN 'JPY'
        ELSE 'USD'
    END,
    -- Balance: phân bố lognormal-ish - phần lớn nhỏ, vài account lớn
    CASE
        WHEN c.customer_id % 30 = 0 THEN ROUND((random() * 500000 + 50000)::numeric, 2)  -- whale
        WHEN c.customer_id % 5 = 0  THEN ROUND((random() * 50000 + 5000)::numeric, 2)    -- medium
        ELSE                              ROUND((random() * 5000 + 100)::numeric, 2)     -- normal
    END,
    -- Status: phần lớn active, ít frozen/closed
    CASE
        WHEN c.customer_id % 50 = 0 THEN 'frozen'
        WHEN c.customer_id % 73 = 0 THEN 'closed'
        ELSE 'active'
    END
FROM customers c
-- Số account / customer = 1 + (customer_id % 3) → 1, 2, hoặc 3
CROSS JOIN LATERAL generate_series(1, 1 + (c.customer_id % 3)) AS n(num);

-- Verify counts và phân bố
SELECT 'customers' AS tbl, COUNT(*) AS cnt FROM customers
UNION ALL
SELECT 'accounts',    COUNT(*) FROM accounts
UNION ALL
SELECT 'transactions',COUNT(*) FROM transactions
UNION ALL
SELECT 'transfers',   COUNT(*) FROM transfers;

-- Phân bố tài khoản theo type và currency
SELECT account_type, currency, COUNT(*) AS cnt,
       ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY account_type, currency
ORDER BY account_type, currency;
