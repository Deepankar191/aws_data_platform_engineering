-- =============================================================================
-- Sample seed: internal customer_profile (the IDENTITY SPINE)
-- =============================================================================
-- Source system : PostgreSQL (captured to bronze via Debezium CDC -> Kafka)
-- Bronze landing : bronze/customer_profile/   (SPEC.md §2, §3)
-- Spine key      : internal_customer_uuid  (canonical id, SPEC.md §6.1)
--
-- PII tagging (SPEC.md §10):
--   emirates_id                         = PII Level 1
--   phone, email, date_of_birth, address = PII Level 2
--
-- This file seeds 7 customers. Their emirates_id / phone / email / full_name /
-- date_of_birth deliberately tie back to the AECB, fraud and AML sample files so
-- a reviewer can trace one customer end-to-end through identity resolution.
--
-- INTENTIONAL CONFLICT (so §6 survivorship / probabilistic fallback is testable):
--   Customer 3 (Rajesh Kumar) has phone +971503456789 here, but the fraud feed
--   reports +971503456700 for the same customer (email still matches). Per §6 the
--   fraud row matches on email only -> candidate -> probabilistic scorer, and
--   POSTGRES wins demographics under SURVIVORSHIP_PRIORITY.
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_profile (
    internal_customer_uuid  UUID PRIMARY KEY,
    emirates_id             VARCHAR(20)  NOT NULL,   -- PII Level 1
    full_name               VARCHAR(200) NOT NULL,   -- PII Level 2
    date_of_birth           DATE         NOT NULL,   -- PII Level 2
    phone                   VARCHAR(20)  NOT NULL,   -- PII Level 2 (E.164)
    email                   VARCHAR(200) NOT NULL,   -- PII Level 2
    monthly_income_aed      NUMERIC(18,2),           -- money: DECIMAL(18,2)
    kyc_completed           BOOLEAN      NOT NULL DEFAULT FALSE,
    address                 TEXT,                    -- PII Level 2
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Debezium needs REPLICA IDENTITY FULL to emit "before" images on updates/deletes.
ALTER TABLE customer_profile REPLICA IDENTITY FULL;

INSERT INTO customer_profile (
    internal_customer_uuid, emirates_id, full_name, date_of_birth, phone, email,
    monthly_income_aed, kyc_completed, address, created_at, updated_at
) VALUES
-- 1 --------------------------------------------------------------------------
('11111111-1111-4111-8111-111111111111', '784-1988-1234567-1', 'Ahmed Al Mansoori',
 '1988-03-14', '+971501234567', 'ahmed.almansoori@example.ae',
 32000.00, TRUE, 'Villa 12, Al Wasl Road, Dubai',
 '2025-01-10T09:12:00+04:00', '2025-03-28T14:30:00+04:00'),
-- 2 --------------------------------------------------------------------------
('22222222-2222-4222-8222-222222222222', '784-1992-2345678-2', 'Fatima Hassan',
 '1992-07-22', '+971502345678', 'fatima.hassan@example.ae',
 18500.00, TRUE, 'Apt 804, Marina Gate 1, Dubai Marina',
 '2025-01-15T11:00:00+04:00', '2025-03-30T08:05:00+04:00'),
-- 3  (CONFLICT: fraud feed reports a different phone for this customer) -------
('33333333-3333-4333-8333-333333333333', '784-1985-3456789-3', 'Rajesh Kumar',
 '1985-11-30', '+971503456789', 'rajesh.kumar@example.ae',
 27000.00, TRUE, 'Office 210, Business Bay, Dubai',
 '2025-02-01T10:20:00+04:00', '2025-03-31T16:45:00+04:00'),
-- 4 --------------------------------------------------------------------------
('44444444-4444-4444-8444-444444444444', '784-1995-4567890-4', 'Sara Abdullah',
 '1995-01-09', '+971504567890', 'sara.abdullah@example.ae',
 15000.00, FALSE, 'Flat 3B, Al Nahda 2, Sharjah',
 '2025-02-05T13:40:00+04:00', '2025-03-29T09:15:00+04:00'),
-- 5 --------------------------------------------------------------------------
('55555555-5555-4555-8555-555555555555', '784-1979-5678901-5', 'Mohammed Ali',
 '1979-06-18', '+971505678901', 'mohammed.ali@example.ae',
 45000.00, TRUE, 'Villa 88, Khalifa City A, Abu Dhabi',
 '2025-02-10T08:00:00+04:00', '2025-03-27T12:00:00+04:00'),
-- 6 --------------------------------------------------------------------------
('66666666-6666-4666-8666-666666666666', '784-1990-6789012-6', 'Priya Nair',
 '1990-09-03', '+971506789012', 'priya.nair@example.ae',
 21000.00, TRUE, 'Apt 1502, JLT Cluster D, Dubai',
 '2025-02-18T15:25:00+04:00', '2025-03-30T18:50:00+04:00'),
-- 7  (AML feed spells the surname slightly differently -> fuzzy/soundex path) -
('77777777-7777-4777-8777-777777777777', '784-1983-7890123-7', 'Omar Sheikh',
 '1983-12-25', '+971507890123', 'omar.sheikh@example.ae',
 38000.00, FALSE, 'Villa 5, Al Rehab, Ajman',
 '2025-03-01T07:30:00+04:00', '2025-03-31T20:10:00+04:00');

-- Example CDC-generating updates a reviewer can run after starting Debezium
-- (each produces an "u" change event on the credit.public.customer_profile topic):
-- UPDATE customer_profile SET kyc_completed = TRUE,  updated_at = now()
--   WHERE internal_customer_uuid = '44444444-4444-4444-8444-444444444444';
-- UPDATE customer_profile SET monthly_income_aed = 47000.00, updated_at = now()
--   WHERE internal_customer_uuid = '55555555-5555-4555-8555-555555555555';
